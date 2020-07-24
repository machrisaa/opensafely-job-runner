import logging
import os

from tinynetrc import Netrc


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
    print("................", name)
    return logger


def set_auth():
    """Set HTTP auth (used by `requests`)
    """
    netrc_path = os.path.join(os.path.expanduser("~"), ".netrc")
    if not os.path.exists(netrc_path):
        with open(netrc_path, "w") as f:
            f.write("")
    netrc = Netrc()
    if netrc["github.com"]["password"]:
        login = netrc["github.com"]["login"]
        password = netrc["github.com"]["password"]
    else:
        password = os.environ["PRIVATE_REPO_ACCESS_TOKEN"]
        login = "doesntmatter"
        netrc["github.com"] = {
            "login": login,
            "password": password,
        }
        netrc.save()
    return (login, password)
