## How to run

	# Install pyspark
	pip install -r contrib/egrul/requirements.txt
	spark-submit --master 'local[4]' -c "spark.driver.memory=4g" --py-files contrib/egrul/egrul_xml.py,contrib/egrul/address.py,contrib/egrul/schema.py contrib/egrul/generate.py


## How to run on macOS

	brew install openjdk@11
	export JAVA_HOME=$(/usr/libexec/java_home -v 11)
