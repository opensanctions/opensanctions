
## Source-data format

The XML format we parse is defined by FNS (Russian Federal Tax Service).
See [`docs/README.md`](docs/README.md) for the vendored XSDs, FNS order
references, and pointers to upcoming format versions.

## How to run

Install a JVM (on macOS):

	brew install openjdk@21
	export JAVA_HOME=$(/usr/libexec/java_home -v 21)

New source archives arrive in `gs://egrul.opensanctions.org` on their own: a systemd
timer on the workspace VM (whose IP is allow-listed at the source) runs
`operations/workspace/sync-egrul.sh` daily at 03:00, which mirrors any new zips from
the source into the bucket.

Run:

	# Install pyspark
	uv pip install -r contrib/egrul/requirements.txt
	# Use a persistent local cache of the source bucket
	mkdir ~/egrul.opensanctions.org-cache
	export LOCAL_BUCKET_CACHE_DIR="$HOME/egrul.opensanctions.org-cache"

	# Run the job! See the script for what the Spark tuning flags are for.
	contrib/egrul/run.sh


## How current state is assembled

A yearly FULL archive lists every company; a daily archive lists only the companies that
changed that day. So a company's absence from an archive says nothing, but an ownership or
directorship absent from a record that *is* in that archive has ended — the registry
described the company and didn't mention it.

That makes end dates a per-company question, so the job doesn't fold archives into each
other. Instead it:

1. parses each archive date into its own table (`2026_03_11`),
2. reduces all of them to skinny appearance tables — which companies, ownerships and
   directorships each archive listed (`company_appearances`, `relationship_appearances`,
   `directorship_successor_starts`). These are bucketed by company id, which is what lets
   every later stage join, window and group without shuffling,
3. groups those by company to find each relationship's *tenures*: runs of consecutive
   appearances of its company that list it (`relationship_tenures`). The company's next
   appearance after a tenure is the archive that ended it, and the day before that is the
   end date,
4. joins the tenures back onto the records to pick up each relationship as the last
   archive that listed it described it, and writes `current_<last archive date>`.

All of these are Hive tables, so a run can be interrupted and resumed, and intermediate
state can be queried with SQL. Dropping a table recomputes it on the next run.


## Provenance

Every row in the output CSVs has an `origin` field naming the source files it was
built from, as `<archive zip name>/<XML file name within the zip>`, comma-separated
if there is more than one. Combined with `seen_date` that's enough to find the file
in the source bucket. Rows with several origins are the ones assembled from more
than one archive: an ownership or directorship that ended carries both the archive
that last listed it and the archive that stopped listing it.


To see the source XML behind a record, feed an `origin` back to the debug tool:

	python contrib/egrul/debug/extract_entity.py \
		"$LOCAL_BUCKET_CACHE_DIR/egrul/EGRUL_408/15.03.2026/EGRUL408_2026-03-15_1.zip" \
		2630027580


## Copy finished data to internal-data bucket

Until this runs as a cronjobs, here is how:

    gcloud storage cp -r --gzip-local-all ~/egrul.opensanctions.org-cache/ru_egrul/processed/current_2025_01_14 gs://internal-data.opensanctions.org/ru_egrul/processed_2025-01-14
