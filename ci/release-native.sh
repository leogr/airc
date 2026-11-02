#!/usr/bin/env bash
# Review proposal: run only on dedicated native CI workers, never active boxes.
set -euo pipefail
exec python3 "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/release-native.py" "$@"
