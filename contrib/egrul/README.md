
## How to run

Get the data:

	mkdir -p ~/internal-data/ru_egrul/egrul.itsoft.ru/EGRUL_406/
	gsutil rsync -r gs://internal-data.opensanctions.org/ru_egrul/egrul.itsoft.ru/EGRUL_406/ ~/internal-data/ru_egrul/egrul.itsoft.ru/EGRUL_406/

Run:

	# Install pyspark
	pip install -r contrib/egrul/requirements.txt
	spark-submit --master 'local[*]' -c "spark.driver.memory=10g" --py-files contrib/egrul/egrul_xml.py,contrib/egrul/address.py,contrib/egrul/schema.py contrib/egrul/generate.py

The checkpoint directory fills up quickly, don't know why yet.

	rm -rf env/spark-checkpoint


## How to run on macOS

	brew install openjdk@11
	export JAVA_HOME=$(/usr/libexec/java_home -v 11)


## Copy finished data to internal-data bucket

Until this runs as a cronjobs, here is how:

    gsutil cp -rZ ~/internal-data/ru_egrul/processed/current_2025_01_14 gs://internal-data.opensanctions.org/ru_egrul/processed_2025-01-14
