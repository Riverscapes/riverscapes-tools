#! /bin/bash
set -eu
.venv/bin/pip install -e ./lib/commons
for f in *; do
    if [ -d "$f" ]; then
        # $f is a directory
        .venv/bin/pip install -e ./lib/commons
    fi
done
