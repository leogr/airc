#!/bin/sh
# Default PLAN is read-only. Only the reviewed protected job sets RELEASE_MODE=apply.
set -eu
exec python3 -I "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/publish-release.py" "$@"
