from unittest.mock import patch
import os
import tempfile

import pytest

from runner.actions import make_container_name
from runner.actions import make_volume_name
from runner.actions import parse_project_yaml
from runner.exceptions import DependencyNotFinished
from runner.exceptions import DuplicateRunInProjectFile
from runner.exceptions import InvalidRunInProjectFile
from runner.exceptions import InvalidVariableInProjectFile
from runner.exceptions import OperationNotInProjectFile


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", "/tmp/storage/highsecurity")
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", "/tmp/storage/mediumsecurity")


def test_make_volume_name():
    repo = "https://github.com/opensafely/hiv-research/"
    branch = "feasibility-no"
    db_flavour = "full"
    assert (
        make_volume_name(repo, branch, db_flavour) == "hiv-research-feasibility-no-full"
    )


def test_bad_volume_name_raises():
    bad_name = "/badname"
    assert make_container_name(bad_name) == "badname"


def test_job_to_project_nodeps(mock_env):
    """Does project information get added to a job correctly in the happy
    path?

    """
    project_path = "tests/fixtures/simple_project_1"
    job = {
        "operation": "generate_cohorts",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }

    project = parse_project_yaml(project_path, job)
    assert project["docker_invocation"] == [
        "--volume",
        "/tmp/storage/highsecurity/repo-master-full:/tmp/storage/highsecurity/repo-master-full",
        "docker.pkg.github.com/opensafely/cohort-extractor/cohort-extractor:latest",
        "generate_cohort",
        "--database-url=sqlite:///test.db",
        "--output-dir=/workspace",
    ]
    assert project["outputs"]["cohort"] == "input.csv"


def test_project_dependency_exception(mock_env):
    """Do incomplete dependencies raise an exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with pytest.raises(DependencyNotFinished) as e:
        parse_project_yaml(project_path, job)
    assert (
        e.value.args[0]
        == "No output for generate_cohorts at /tmp/storage/highsecurity/repo-master-full/input.csv"
    )


@patch("runner.actions.make_path")
def test_project_dependency_no_exception(dummy_output_path, mock_env):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with tempfile.TemporaryDirectory() as d:
        dummy_output_path.return_value = d
        with open(os.path.join(d, "input.csv"), "w") as f:
            f.write("")
        project = parse_project_yaml(project_path, job)
        assert project["docker_invocation"] == [
            "--volume",
            f"{d}:{d}",
            "--volume",
            f"{d}:{d}",
            "docker.pkg.github.com/opensafely/stata-docker/stata-mp:latest",
            "analysis/model.do",
            f"{d}/input.csv",
        ]
        assert project["outputs"]["log"] == "model.log"


def test_operation_not_in_project(mock_env):
    """Do jobs whose operation is not specified in a project raise an
    exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job = {
        "operation": "do_the_twist",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
    }
    with pytest.raises(OperationNotInProjectFile):
        parse_project_yaml(project_path, job)


def test_duplicate_operation_in_project(mock_env):
    """Do jobs whose operation is duplicated in a project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_1"
    job = {
        "operation": "run_model_1",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with pytest.raises(DuplicateRunInProjectFile):
        parse_project_yaml(project_path, job)


def test_invalid_run_in_project(mock_env):
    """Do jobs with unsupported run commands in their project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_2"
    job = {
        "operation": "run_model_1",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with pytest.raises(InvalidRunInProjectFile):
        parse_project_yaml(project_path, job)


def test_valid_run_in_project(mock_env):
    """Do run commands in jobs get their variables interpolated?

    """
    project_path = "tests/fixtures/simple_project_2"
    job = {
        "operation": "generate_cohort",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    project = parse_project_yaml(project_path, job)
    assert project["docker_invocation"] == [
        "--volume",
        "/tmp/storage/highsecurity/repo-master-full:/tmp/storage/highsecurity/repo-master-full",
        "docker.pkg.github.com/opensafely/cohort-extractor/cohort-extractor:foo",
        "generate_cohort",
        "--database-url=sqlite:///test.db",
        "--output-dir=/workspace",
    ]


@patch("runner.actions.make_path")
def test_project_output_missing_raises_exception(dummy_output_path, mock_env):
    """Do user-supplied variables that reference non-existent outputs
    raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_3"
    job = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with tempfile.TemporaryDirectory() as d:
        dummy_output_path.return_value = d
        with open(os.path.join(d, "input.csv"), "w") as f:
            f.write("")
        with pytest.raises(InvalidVariableInProjectFile):
            parse_project_yaml(project_path, job)


@patch("runner.actions.make_path")
def test_bad_variable_path_raises_exception(dummy_output_path, mock_env):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_4"
    job = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with tempfile.TemporaryDirectory() as d:
        dummy_output_path.return_value = d
        with open(os.path.join(d, "input.csv"), "w") as f:
            f.write("")
        with pytest.raises(InvalidVariableInProjectFile):
            parse_project_yaml(project_path, job)
