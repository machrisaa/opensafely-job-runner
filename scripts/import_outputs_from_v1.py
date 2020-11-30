"""
For importing files from the old job-runner layout to the new

Takes a list of workspace slugs as arguments and writes a bash script to stdout
which should be executed outside of Docker to avoid crippled I/O performance.

Once the bash script has been run this same command should be executed again
and the `manifest.json` file will be updated appropriately (which is necessary
for the new system to "see" the output files).
"""
import json
import os
from pathlib import Path
import subprocess
import sys


# Directory inside working directory where manifest and logs are created
METADATA_DIR = "metadata"

# Records details of which action created each file
MANIFEST_FILE = "manifest.json"


def import_old_files(workspace_name):
    outputs = find_outputs_for_workspace(workspace_name)
    copy_outputs_to_workspace(workspace_name, outputs)


def find_outputs_for_workspace(workspace_name):
    outputs = {}
    high_priv_base = fix_path(os.environ["HIGH_PRIVACY_STORAGE_BASE"])
    med_priv_base = fix_path(os.environ["MEDIUM_PRIVACY_STORAGE_BASE"])
    for privacy_level, base_dir in [
        ("highly_sensitive", high_priv_base),
        ("moderately_sensitive", med_priv_base),
    ]:
        workspace_dirs = list(base_dir.glob(f"tpp-https-*-full-{workspace_name}"))
        if len(workspace_dirs) != 1:
            raise RuntimeError(f"Expected exactly one match, got: {workspace_dirs}")
        else:
            workspace_dir = str(workspace_dirs[0])
        response = subprocess.run(
            ["find", workspace_dir, "-type", "f"],
            check=True,
            capture_output=True,
            encoding="utf-8",
        )
        files = response.stdout.splitlines()
        for filename in files:
            parts = filename[len(workspace_dir) + 1 :].split("/")
            action = parts[0]
            output_name = "/".join(parts[2:])
            outputs[output_name] = {
                "created_by_action": action,
                "privacy_level": privacy_level,
                "source_path": filename,
            }
    return outputs


def copy_outputs_to_workspace(workspace_name, outputs):
    workspace_dir = (
        fix_path(os.environ["HIGH_PRIVACY_STORAGE_BASE"])
        / "workspaces"
        / workspace_name
    )
    print(f"# Copying files for {workspace_name}")
    print()
    missing = False
    for output_name, details in outputs.items():
        output_path = workspace_dir / output_name
        if not output_path.exists():
            output_file = str(output_path)
            missing = True
            print(f"# Copying {workspace_name}:{output_name}")
            print(f"if [[ ! -f '{output_file}' ]]; then")
            print(f"  echo 'Copying {details['source_path']}'")
            print(f"  mkdir -p '{str(output_path.parent)}'")
            print(f"  cp '{details['source_path']}' '{output_file}.tmp'")
            print(f"  mv --no-clobber '{output_file}.tmp' '{output_file}'")
            print("fi")
            print()
    if not missing:
        print(f"# Updating manifest for {workspace_name}")
        manifest = read_manifest_file(workspace_dir)
        modified = update_manifest(manifest, outputs)
        if modified:
            write_manifest_file(workspace_dir, manifest)
    else:
        print(
            f"# Files need copying before manifest for {workspace_name} can be updated"
        )


def read_manifest_file(workspace_dir):
    """
    Read the manifest of a given workspace, returning an empty manifest if none
    found
    """
    try:
        with open(workspace_dir / METADATA_DIR / MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"files": {}, "actions": {}}


def update_manifest(manifest, new_outputs):
    modified = False
    files = manifest["files"]
    action_names = set()
    for name, details in new_outputs.items():
        if name not in files:
            files[name] = {
                "created_by_action": details["created_by_action"],
                "privacy_level": details["privacy_level"],
            }
            action_names.add(details["created_by_action"])
            modified = True
    manifest["files"] = dict(sorted(files.items()))
    for action in action_names:
        if action not in manifest["actions"]:
            manifest["actions"][action] = {
                "state": "succeeded",
                "commit": "unknown",
                "docker_image_id": "unknown",
                "job_id": "unknown",
                "run_by_user": "unknown",
                "created_at": "unknown",
                "completed_at": "unknown",
            }
            modified = True
    return modified


def write_manifest_file(workspace_dir, manifest):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    manifest_file_tmp = manifest_file.with_suffix(".tmp")
    manifest_file_tmp.parent.mkdir(parents=True, exist_ok=True)
    manifest_file_tmp.write_text(json.dumps(manifest, indent=2))
    manifest_file_tmp.replace(manifest_file)


def log(msg):
    print(msg, file=sys.stderr)


def fix_path(path):
    return Path(path).resolve()


if __name__ == "__main__":
    print("#!/bin/bash")
    print("set -eo pipefail\n\n")
    for workspace_name in sys.argv[1:]:
        import_old_files(workspace_name)
