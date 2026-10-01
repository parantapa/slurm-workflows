#!/bin/bash
# This script is the author's own build wrapper.
# The project does not need it to build.
#
# Expects the repository root as the working directory.
# Takes one command, and runs the matching run_<command> function.
# Exits 1 on an unknown command,
# and otherwise with the status of the command.
#
# Usage: scripts/pb-dev.sh (help | command)

set -Eeuo pipefail

# Builds the sdist and the wheel into dist/,
# and checks them with twine.
# The package is pure Python,
# so one wheel serves every platform.
run_build-python-package() {
    set -x
    python -m build
    python -m twine check dist/*.tar.gz dist/*.whl
}

# Uploads the sdist and the wheel in dist/ with twine.
# Nothing cleans dist/, so this command also uploads the files
# that earlier builds left there.
run_upload-python-package() {
    set -x
    python -m twine upload dist/*.tar.gz dist/*.whl
}

# Prints the usage and the list of commands.
show_help() {
    echo "Usage: $0 (help | command)"
    echo
    echo "Available commands:"
    echo "    help"

    # show_help builds the command list from the run_* functions,
    # so a new command needs no change here.
    local fn
    while read -r fn; do
        echo "    ${fn#run_}"
    done < <(declare -F | awk '{print $3}' | grep '^run_' | sort)
}

if [[ $# -eq 0 || "${1:-}" == "help" || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    show_help
elif [[ $(type -t "run_${1}") == function ]]; then
    fn="run_${1}"
    shift
    $fn "$@"
else
    echo "Unknown command: $1" >&2
    show_help >&2
    exit 1
fi
