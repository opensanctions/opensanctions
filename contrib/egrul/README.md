
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

	# Run the job!
	spark-submit --master 'local[*]' -c "spark.driver.memory=10g" --py-files contrib/egrul/egrul_xml.py,contrib/egrul/address.py,contrib/egrul/parse_context.py,contrib/egrul/schema.py contrib/egrul/generate.py

The checkpoint directory fills up quickly, don't know why yet.

	rm -rf env/spark-checkpoint


## Provenance

Every row in the output CSVs has an `origin` field naming the source files it was
built from, as `<archive zip name>/<XML file name within the zip>`, comma-separated
if there is more than one. Combined with `seen_date` that's enough to find the file
in the source bucket. Rows with several origins are the ones assembled from more
than one archive: an ownership or directorship that ended carries both the archive
that last listed it and the archive that stopped listing it.


## Copy finished data to internal-data bucket

Until this runs as a cronjobs, here is how:

    gcloud storage cp -r --gzip-local-all ~/egrul.opensanctions.org-cache/ru_egrul/processed/current_2025_01_14 gs://internal-data.opensanctions.org/ru_egrul/processed_2025-01-14
