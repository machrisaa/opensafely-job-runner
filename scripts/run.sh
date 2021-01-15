#!/bin/bash
# This is a wrapper script used to invoke the job-runner on Windows with the
# environment set correctly. It's assumed to be run in the working dir of a
# job-runner checkout with copies of all dependencies in a "lib" directory.
#
# Usage example, from inside git-bash
#
#   ./scripts/run.sh -m jobrunner.service

# set -a means all declared variables are exported
set -a
source .env
set +a
export PYTHONPATH="lib"
exec "C:\\Program Files\\Python39\\python" "$@"
