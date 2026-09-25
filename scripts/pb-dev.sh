#!/bin/bash
# The author's own build wrapper. Not required to build the project.
#
# Runs from the repository root.
# Takes one command, and runs the run_<command> function below.
# Exits 1 on an unknown command,
# and otherwise with the status of the command.
#
# Usage: scripts/pb-dev.sh (help | command)

set -Eeuo pipefail

# Build the sdist and the wheel into dist/,
# and check them with twine.
# The package is pure Python,
# so one wheel serves every platform.
run_build-python-package() {
    set -x
    python -m build
    python -m twine check dist/*.tar.gz dist/*.whl
}

# Upload the sdist and the wheel in dist/ with twine.
run_upload-python-package() {
    set -x
    python -m twine upload dist/*.tar.gz dist/*.whl
}

# Print the usage and the list of commands.
show_help() {
    echo "Usage: $0 (help | command)"
    echo
    echo "Available commands:"
    echo "    help"

    # The command list is derived from the run_* functions,
    # so adding a command needs no change here.
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
