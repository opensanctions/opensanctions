#!/usr/bin/env bash
# Run the EGRUL Spark job against the local warehouse.
set -euo pipefail

# The --py-files paths and the dataset .yml lookup are relative to the repo root.
cd "$(dirname "$0")/../.."
# Spark needs a JVM it supports; the default on macOS may be much newer.
export JAVA_HOME=$(/usr/libexec/java_home -v 21)

spark-submit --master 'local[*]' \
	-c spark.driver.memory=15g \
	`# The per-archive tables are thousands of small Parquet files; pack more per task.` \
	-c spark.sql.files.openCostInBytes=32m \
	`# Only ~13% of the relationship rows shuffled into the final join survive it, so let` \
	`# Spark bloom-filter the wide side first. The defaults are too small to trigger on` \
	`# tables this size. Unverified - check for BloomFilterMightContain in the plan.` \
	-c spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold=2g \
	-c spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold=1g \
	--py-files contrib/egrul/egrul_xml.py,contrib/egrul/address.py,contrib/egrul/parse_context.py,contrib/egrul/schema.py \
	contrib/egrul/generate.py
