
## Source-data format

The XML format we parse is defined by FNS (Russian Federal Tax Service).
See [`docs/README.md`](docs/README.md) for the vendored XSDs, FNS order
references, and pointers to upcoming format versions.

The source archives live in the `gs://egrul.opensanctions.org` bucket,
synced from FNS by a separate job. No local data fetch is needed before
running.

## How to run locally

Install a JVM (on macOS):

    brew install openjdk@21
    export JAVA_HOME=$(/usr/libexec/java_home -v 21)

Install pyspark and friends:

    pip install -r contrib/egrul/requirements.txt

Set up a persistent local archive cache so re-runs don't re-download
hundreds of GB of zips:

    mkdir ~/internal-data
    export LOCAL_BUCKET_CACHE_DIR="$HOME/internal-data"

`LOCAL_BUCKET_CACHE_DIR` is opt-in: when set, source zips are cached on
disk under that path. When unset (e.g. on serverless workers), the worker
streams each zip into memory and discards it.

Run the job:

    spark-submit --master 'local[*]' \
      --conf spark.driver.memory=10g \
      --conf spark.sql.catalogImplementation=hive \
      --py-files contrib/egrul/archives.py,contrib/egrul/egrul_xml.py,contrib/egrul/address.py,contrib/egrul/schema.py \
      contrib/egrul/generate.py

`spark.sql.catalogImplementation=hive` is required locally so the
`saveAsTable`/`tableExists` resume pattern persists across runs
(otherwise tables live in an in-memory catalog that vanishes with
the process).

The job writes the final partitioned CSVs straight to
`gs://internal-data.opensanctions.org/ru_egrul/processed/<run timestamp>/`.
