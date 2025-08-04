
## How to run

Install a JVM (on macOS):

	brew install openjdk@21
	export JAVA_HOME=$(/usr/libexec/java_home -v 21)

Get some new data. This must be done on rivne.

	pushd ~/operations/workspace/; ./sync-egrul.sh; popd

Run:

	# Install pyspark
	pip install -r contrib/egrul/requirements.txt
	# Use a persistent local cache of the internal-data bucket
	mkdir ~/internal-data
	export LOCAL_BUCKET_CACHE_DIR="$HOME/internal-data"

	# Run the job!
	spark-submit --master 'local[*]' -c "spark.driver.memory=10g" --py-files contrib/egrul/egrul_xml.py,contrib/egrul/address.py,contrib/egrul/schema.py contrib/egrul/generate.py

The checkpoint directory fills up quickly, don't know why yet.

	rm -rf env/spark-checkpoint


## Copy finished data to internal-data bucket

Until this runs as a cronjobs, here is how:

    gsutil cp -rZ ~/internal-data/ru_egrul/processed/current_2025_01_14 gs://internal-data.opensanctions.org/ru_egrul/processed_2025-01-14
