#!/usr/bin/env bash
# Replace the local resolver table with the latest production dump.
set -euo pipefail

BUCKET=gs://prod-etl-dev-dumps.opensanctions.org
exec "$(dirname "$0")/load_tables.sh" "resolver=$BUCKET/resolver/resolver.csv.gz"
