
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

## How to run on Google Cloud Managed Service for Apache Spark

This is the production path. The job runs as a serverless batch (formerly
Dataproc Serverless), parses archives in parallel, and writes the output
CSVs straight to GCS.

### One-time setup

Required infrastructure (defined in `operations/tf/etl/egrul_spark.tf`):

- A service account `etl-egrul-spark@<project>.iam.gserviceaccount.com`
  with `roles/dataproc.worker`, `roles/logging.logWriter`, read access on
  `gs://egrul.opensanctions.org`, and `objectAdmin` on
  `gs://internal-data.opensanctions.org`.
- `gcloud services enable dataproc.googleapis.com` on the project.

The custom container image (`contrib/egrul/Dockerfile`) extends the root
opensanctions image and adds what Managed Service for Apache Spark needs.
It is pushed to GHCR as `ghcr.io/opensanctions/opensanctions-egrul-spark`.

### Build the image (manual)

The image isn't built on every commit since it's only used when running a
batch. Trigger a build manually before submitting:

    gh workflow run build-egrul-spark.yml

This pulls the latest root image, layers our Spark-specific bits on top,
and pushes to GHCR. Takes a couple of minutes.

### Upload the entrypoint stub (one-time)

`gcloud dataproc batches submit pyspark` requires a main Python file URI.
We use a tiny stub (`entrypoint.py`) that imports and calls `generate.main`
from the image's PYTHONPATH, so this gets uploaded once and never needs
to change — code changes are picked up by rebuilding the image.

    gsutil cp contrib/egrul/entrypoint.py \
      gs://internal-data.opensanctions.org/_dataproc_staging/entrypoint.py

### Submit a batch

    gcloud dataproc batches submit pyspark \
      gs://internal-data.opensanctions.org/_dataproc_staging/entrypoint.py \
      --project=<project> \
      --region=<region> \
      --version=3.0 \
      --container-image=ghcr.io/opensanctions/opensanctions-egrul-spark:latest \
      --service-account=etl-egrul-spark@<project>.iam.gserviceaccount.com \
      --properties=spark.executor.instances=50,spark.sql.shuffle.partitions=200

The batch page in the Dataproc UI shows progress and the DCU-hour total
at the bottom (that's the cost). Typical wall-clock with 50 executors is
1.5–2 hours.
