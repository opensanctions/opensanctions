#!/usr/bin/env bash
# Installs zavod's dev environment on macOS (arm64).
#
# Two native packages need special handling:
#   pyicu  - must build from source; no pre-built wheel exists for this platform.
#   plyvel - homebrew leveldb is built -fno-rtti, so its RTTI symbols are
#            absent; the pre-built wheel expects them. Fix: build from source
#            with matching -fno-rtti. Requires two passes since pyicu can't
#            be built with -fno-rtti (ICU headers use dynamic_cast).
set -euo pipefail

ICU="/opt/homebrew/opt/icu4c"
LEVELDB="/opt/homebrew/opt/leveldb"

if [[ ! -d "$ICU" ]]; then
    echo "error: icu4c not found at $ICU — run: brew install icu4c" >&2
    exit 1
fi
if [[ ! -d "$LEVELDB" ]]; then
    echo "error: leveldb not found at $LEVELDB — run: brew install leveldb" >&2
    exit 1
fi

echo "--- Step 1: installing dependencies (pyicu and plyvel built from source) ---"
PATH="$ICU/bin:$PATH" \
CPPFLAGS="-I$LEVELDB/include" \
LDFLAGS="-L$LEVELDB/lib" \
uv sync --python 3.13 --no-binary-package pyicu --no-binary-package plyvel --extra dev --extra docs --no-cache

echo "--- Step 2: rebuilding plyvel from source (-fno-rtti) ---"
CXXFLAGS="-fno-rtti" \
LDFLAGS="-L$LEVELDB/lib" \
CPPFLAGS="-I$LEVELDB/include" \
uv pip install --no-binary plyvel --reinstall plyvel==1.5.1 --no-cache

echo "--- Verifying ---"
uv run python -c "import plyvel; print('plyvel ok')"
uv run python -c "import icu; print('icu ok')"
uv run zavod --help > /dev/null
echo "Done."
