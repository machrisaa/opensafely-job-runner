from pathlib import Path
from urllib.parse import urlparse

import logging
import os
import re


def getlogger(name):
    # Create a logger with a field for recording a unique job id, and a
    # `baselogger` adapter which fills this field with a hyphen, for use
    # when logging events not associated with jobs
    FORMAT = "%(asctime)-15s %(levelname)-10s  %(job_id)-10s %(message)s"
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


def get_auth():
    return (os.environ["QUEUE_USER"], os.environ["QUEUE_PASS"])


def safe_join(startdir, path):
    requested_path = os.path.normpath(os.path.join(startdir, path))
    startdir = str(startdir)  # Normalise from PosixPath
    assert (
        os.path.commonprefix([requested_path, startdir]) == startdir
    ), f"Invalid requested path {requested_path}, not in {startdir}"
    return requested_path


def make_volume_name(workspace):
    """Create a string suitable for naming a folder that will contain
    data, using state related to the current workspace as a unique key.

    """
    parts = []
    for key in ["repo", "branch", "db", "owner", "name"]:
        # Remove symbols (excluding hyphens)
        parts.append(re.sub(r"[^0-9a-z-]", "-", workspace[key]))
    # Dedupe hyphens
    parts = "-".join(parts)
    parts = re.sub(r"--+", "-", parts)
    return parts


def make_output_path(action, privacy_level, filename):
    volume_name = make_volume_name(action["workspace"])
    if privacy_level == "highly_sensitive":
        storage_base = Path(os.environ["HIGH_PRIVACY_STORAGE_BASE"])
    elif privacy_level == "moderately_sensitive":
        storage_base = Path(os.environ["MEDIUM_PRIVACY_STORAGE_BASE"])
    output_bucket = storage_base / volume_name
    output_bucket.mkdir(parents=True, exist_ok=True)
    return safe_join(output_bucket, filename)


def all_output_paths_for_action(action):
    for privacy_level, outputs in action.get("outputs", {}).items():
        for output_name, output_filename in outputs.items():
            yield privacy_level, output_name, make_output_path(
                action, privacy_level, output_filename
            )
