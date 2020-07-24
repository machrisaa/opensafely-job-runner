import copy
import networkx as nx
import os
import re
import shlex
import yaml

from pathlib import Path
from urllib.parse import urlparse

from runner.exceptions import CohortExtractorError
from runner.exceptions import DependencyNotFinished
from runner.exceptions import DuplicateRunInProjectFile
from runner.exceptions import InvalidRunInProjectFile
from runner.exceptions import InvalidVariableInProjectFile
from runner.exceptions import OpenSafelyError
from runner.exceptions import OperationNotInProjectFile
from runner.exceptions import ScriptError

# These numbers correspond to "levels" as described in our security
# documentation
PRIVACY_LEVEL_HIGH = 3
PRIVACY_LEVEL_MEDIUM = 4

# The keys of this dictionary are all the supported `run` commands in
# jobs
RUN_COMMANDS_CONFIG = {
    "cohortextractor": {
        "input_privacy_level": None,
        "output_privacy_level": PRIVACY_LEVEL_HIGH,
        "docker_invocation": [
            "docker.pkg.github.com/opensafely/cohort-extractor/cohort-extractor",
            "generate_cohort",
            "--database-url={database_url}",
            "--output-dir=/workspace",
        ],
        "docker_exception": CohortExtractorError,
    },
    "stata-mp": {
        "input_privacy_level": PRIVACY_LEVEL_HIGH,
        "output_privacy_level": PRIVACY_LEVEL_MEDIUM,
        "docker_invocation": ["docker.pkg.github.com/opensafely/stata-docker/stata-mp"],
        "docker_exception": ScriptError,
    },
}


def make_volume_name(repo, branch_or_tag, db_flavour):
    """Create a string suitable for naming a folder that will contain
    data, using state related to the current job as a unique key.

    """
    repo_name = urlparse(repo).path[1:]
    if repo_name.endswith("/"):
        repo_name = repo_name[:-1]
    repo_name = repo_name.split("/")[-1]
    return repo_name + "-" + branch_or_tag + "-" + db_flavour


def raise_if_unfinished(action):
    """Does the target output file for this job exist?  If not, raise an
    exception.

    """
    for output_name, output_filename in action.get("outputs", {}).items():
        expected_path = os.path.join(action["output_path"], output_filename)
        if not os.path.exists(expected_path):
            msg = f"No output for {action['action_id']} at {expected_path}"
            raise DependencyNotFinished(msg, report_args=True)


def escape_braces(unescaped_string):
    """Escape braces so that they will be preserved through a string
    `format()` operation

    """
    return unescaped_string.replace("{", "{{").replace("}", "}}")


def variables_in_string(string_with_variables, variable_name_only=False):
    """Return a list of variables of the form `${{ var }}` (or `${{var}}`)
    in the given string.

    Setting the `variable_name_only` flag will a list of variables of
    the form `var`

    """
    matches = re.findall(
        r"(\$\{\{ ?([A-Za-z][A-Za-z0-9.-_]+) ?\}\})", string_with_variables
    )
    if variable_name_only:
        return [x[1] for x in matches]
    else:
        return [x[0] for x in matches]


def load_and_validate_project_actions(workdir):
    """Check that a dictionary of project actions is valid
    """
    with open(os.path.join(workdir, "project.yaml"), "r") as f:
        project_actions = yaml.load(f, Loader=yaml.Loader)["actions"]
    seen_runs = []
    for action_id, action_config in project_actions.items():
        # Check it's a permitted run command
        name, version, args = split_and_format_run_command(action_config["run"])
        if name not in RUN_COMMANDS_CONFIG:
            raise InvalidRunInProjectFile(name)

        # Check the run command + args signature appears only once in
        # a project
        run_signature = f"{name}_{args}"
        if run_signature in seen_runs:
            raise DuplicateRunInProjectFile(name, args, report_args=True)
        seen_runs.append(run_signature)

        # Check any variables are supported
        for v in variables_in_string(action_config["run"]):
            if not v.replace(" ", "").startswith("${{needs"):
                raise InvalidVariableInProjectFile(v, report_args=True)
            try:
                _, action_id, outputs_key, output_id = v.split(".")
                if outputs_key != "outputs":
                    raise InvalidVariableInProjectFile(v, report_args=True)
            except ValueError:
                raise InvalidVariableInProjectFile(v, report_args=True)
    return project_actions


def interpolate_variables(args, dependency_actions):
    """Given a list of arguments, interpolate variables using a dotted
    lookup against the supplied dependencies dictionary

    """
    interpolated_args = []
    for arg in args:
        variables = variables_in_string(arg, variable_name_only=True)
        if variables:
            try:
                _, action_id, outputs_key, output_id = variables[0].split(".")
                dependency_action = dependency_actions[action_id]
                dependency_outputs = dependency_action[outputs_key]
                filename = dependency_outputs[output_id]
            except (KeyError, ValueError):
                raise InvalidVariableInProjectFile(
                    f"No output corresponding to {arg} was found", report_args=True
                )
            assert isinstance(
                filename, str
            ), f"Could not find a string value for {filename}"
            arg = os.path.join(dependency_action["output_path"], filename)
        interpolated_args.append(arg)
    return interpolated_args


def split_and_format_run_command(run_command):
    """A `run` command is in the form of `run_token:optional_version [args]`.

    Split this into its constituent parts, with the arguments
    shell-escaped, and any substitution tokens normalized and escaped
    for later parsing and formatting.

    """
    for v in variables_in_string(run_command):
        # Remove spaces to prevent shell escaping from thinking these
        # are different tokens
        run_command = run_command.replace(v, v.replace(" ", ""))
        # Escape braces to prevent python `format()` from dropping
        # doubled braces
        run_command = escape_braces(run_command)

    parts = shlex.split(run_command)
    # Commands are in the form command:version
    if ":" in parts[0]:
        run_token, version = parts[0].split(":")
    else:
        run_token = parts[0]
        version = "latest"

    return run_token, version, parts[1:]


def add_runtime_metadata(action, repo=None, db=None, tag=None, **kwargs):
    """Given a run command specified in project.yaml, validate that it is
    permitted, and return how it should be invoked for `docker run`

    Adds docker_invocation, docker_exception, privacy_level,
    database_url, container_name, and output_path to the `action` dict.

    """
    action = copy.deepcopy(action)
    command = action["run"]
    name, version, args = split_and_format_run_command(command)

    if name not in RUN_COMMANDS_CONFIG:
        raise InvalidRunInProjectFile(name)

    # Convert human-readable database name into DATABASE_URL
    action["database_url"] = os.environ[f"{db.upper()}_DATABASE_URL"]
    info = copy.deepcopy(RUN_COMMANDS_CONFIG[name])

    # Convert the command name into a full set of arguments that can
    # be passed to `docker run`, but preserving user-defined variables
    # in the form `${{ variable }}` for interpolation later (after the
    # dependences have been walked)
    docker_invocation = info["docker_invocation"]
    if version:
        docker_invocation[0] = docker_invocation[0] + ":" + version

    # Every action has an output path; all but those operating
    # directly on the backend also have an input path
    extra_mounts = [
        "--volume",
        "{output_path}:{output_path}",
    ]
    action["output_path"] = make_path(
        repo=repo, tag=tag, db=db, privacy_level=info["output_privacy_level"]
    )
    action["container_name"] = make_container_name(action["output_path"])

    if info["input_privacy_level"]:
        extra_mounts.extend(["--volume", "{input_path}:{input_path}"])
        action["input_path"] = make_path(
            repo=repo, tag=tag, db=db, privacy_level=info["input_privacy_level"]
        )
    docker_invocation = extra_mounts + docker_invocation
    action["docker_exception"] = info["docker_exception"]

    # Interpolate action dictionary into argument template
    docker_invocation = docker_invocation + args

    action["docker_invocation"] = [arg.format(**action) for arg in docker_invocation]
    return action


def parse_project_yaml(workdir, job):
    """Given a checkout of an OpenSAFELY repo containing a `project.yml`,
    check the provided job can run, and if so, update it with
    information about how to run it in a docker container.

    If the job has unfinished dependencies, a DependencyNotFinished
    exception is raised.

    """
    project_actions = load_and_validate_project_actions(workdir)

    requested_action_id = job["operation"]
    if requested_action_id not in project_actions:
        raise OperationNotInProjectFile(requested_action_id)

    # Build dependency graph
    graph = nx.DiGraph()
    for action_id, action_config in project_actions.items():
        project_actions[action_id]["action_id"] = action_id
        graph.add_node(action_id)
        for dependency_id in action_config.get("needs", []):
            graph.add_node(dependency_id)
            graph.add_edge(dependency_id, action_id)
    dependencies = graph.predecessors(requested_action_id)

    # Compute runtime metadata for the job we're interested
    job_action = add_runtime_metadata(project_actions[requested_action_id], **job)

    # Do the same thing for dependencies, and also assert that they've
    # completed by checking their expected output exists
    dependency_actions = {}
    for action_id in dependencies:
        # Adds docker_invocation, docker_exception, privacy_level, and
        # output_path to the config
        action = add_runtime_metadata(project_actions[action_id], **job)
        raise_if_unfinished(action)
        dependency_actions[action_id] = action

    # Now interpolate user-provided variables into docker
    # invocation. This must happen after metadata has been added to
    # the dependencies, as variables can reference the ouputs of other
    # actions
    job_action["docker_invocation"] = interpolate_variables(
        job_action["docker_invocation"], dependency_actions
    )
    job.update(job_action)
    return job


def make_path(repo=None, tag=None, db=None, privacy_level=None):
    """Make a path in a location appropriate to the privacy level,
    using state (as represented by the other keyword args) as a unique
    key

    """
    volume_name = make_volume_name(repo, tag, db)
    # When running this within a docker container, the storage base
    # should be a volume mounted from the docker host; e.g. if the
    # storage base is /mnt/high_privacy, then docker should be started
    # with the option `--volume /mnt/high_privacy:/mnt/high_privacy`.
    #
    # This allows us to contruct an `output_path` value which can be
    # shared directly between a docker host, and a
    # docker-within-docker.
    if privacy_level == PRIVACY_LEVEL_HIGH:
        storage_base = Path(os.environ["HIGH_PRIVACY_STORAGE_BASE"])
    elif privacy_level == PRIVACY_LEVEL_MEDIUM:
        storage_base = Path(os.environ["MEDIUM_PRIVACY_STORAGE_BASE"])
    else:
        raise OpenSafelyError("Unsupported privacy level")
    output_path = storage_base / volume_name
    output_path.mkdir(parents=True, exist_ok=True)
    return str(output_path)


def make_container_name(volume_name):
    # By basing the container name to the volume_name, we are
    # guaranteeing only one identical job can run at once by docker
    container_name = re.sub(r"[^a-zA-Z0-9]", "-", volume_name)
    # Remove any leading dashes, as docker requires images begin with [:alnum:]
    if container_name.startswith("-"):
        container_name = container_name[1:]
    return container_name
