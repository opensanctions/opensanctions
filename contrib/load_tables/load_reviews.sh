#!/usr/bin/env bash
# Replace the local data review tables with the latest production dump.
set -euo pipefail

BUCKET=gs://prod-etl-dev-dumps.opensanctions.org
exec "$(dirname "$0")/load_tables.sh" \
	"review=$BUCKET/data_reviews/review.csv.gz" \
	"review_entity=$BUCKET/data_reviews/review_entity.csv.gz"
