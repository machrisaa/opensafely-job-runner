from unittest.mock import patch
import requests_mock
import runner
import time

from runner.exceptions import OpenSafelyError
from runner.exceptions import RepoNotFound

import pytest


class TestError(OpenSafelyError):
    status_code = 10


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", "/tmp/storage/highsecurity")
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", "/tmp/storage/mediumsecurity")


default_job = {
    "url": "http://test.com/jobs/0/",
    "repo": "myrepo",
    "tag": "mytag",
    "backend": "tpp",
    "db": "full",
    "started": False,
    "operation": "generate_cohort",
    "status_code": None,
    "output_path": "output_path",
    "created_at": None,
    "started_at": None,
    "completed_at": None,
}


class TestJobRunner:
    def __init__(self, job):
        self.job = job

    def __repr__(self):
        return self.__class__.__name__


class WorkingJobRunner(TestJobRunner):
    def __call__(self):
        return self.job


class SlowJobRunner(TestJobRunner):
    def __call__(self):
        time.sleep(1)
        return self.job


class BrokenJobRunner(TestJobRunner):
    def __call__(self):
        raise KeyError


def test_job_list():
    return {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [default_job],
    }


def test_watch_broken_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False, jobrunner=BrokenJobRunner)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {
            "status_code": 99,
            "status_message": "Unclassified error id BrokenJobRunner",
        }


def test_watch_working_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False, jobrunner=WorkingJobRunner)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {
            "output_path": "output_path",
            "status_code": 0,
        }


@patch("runner.HOUR", 0.001)
def test_watch_timeout_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False, jobrunner=SlowJobRunner)
        assert adapter.request_history[0].json()["started"] is True
        assert adapter.request_history[1].json() == {
            "status_code": -1,
            "status_message": "TimeoutError(86400s) id SlowJobRunner",
        }


def test_exception_reporting():
    error = TestError("thing not to leak", report_args=False)
    assert error.safe_details() == "TestError: [possibly-unsafe details redacted]"
    assert repr(error) == "TestError('thing not to leak')"

    error = TestError("thing OK to leak", report_args=True)
    assert error.safe_details() == "TestError: thing OK to leak"
    assert repr(error) == "TestError('thing OK to leak')"


def test_reserved_exception():
    class InvalidError(OpenSafelyError):
        status_code = -1

    with pytest.raises(AssertionError) as e:
        raise InvalidError(report_args=True)
    assert "reserved" in e.value.args[0]

    with pytest.raises(RepoNotFound):
        raise RepoNotFound(report_args=True)
