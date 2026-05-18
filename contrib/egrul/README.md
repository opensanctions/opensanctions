
## Source-data format

The EGRUL/EGRIP XML format is defined by FNS (Russian Federal Tax Service) and
published as XSD schemas. They are stored in `docs/`:

| File | Format | Notes |
|---|---|---|
| `docs/VO_RUGF_2_311_26_04_07_01.xsd` | EGRUL (legal entities) 4.07 | Current. FNS order ЕД-7-14/382@ (2023-06-06). |
| `docs/VO_RIGF_2_311_27_04_06_01.xsd` | EGRIP (sole traders) 4.06 | Current. Same order. |
| `docs/VO_RUGF_2_311_26_04_08_01.xsd` | EGRUL 4.08 | Effective 2026-02-01, mandatory 2026-08-01. FNS order ЕД-7-14/613@ (2025-07-08). |
| `docs/VO_RIGF_2_311_27_04_07_01.xsd` | EGRIP 4.07 | Same order as 4.08 EGRUL. |

The XSDs are `windows-1251`-encoded. To read them in a UTF-8 terminal:

    iconv -f windows-1251 -t utf-8 docs/VO_RUGF_2_311_26_04_07_01.xsd | less

### Where these come from / where to look for future updates

- Landing page (overview, regulations, FTP access procedure):
  https://www.nalog.gov.ru/rn77/service/egrip2/egrip_vzayim/
- Current formats (4.07/4.06), source of the first two files above:
  https://www.nalog.gov.ru/rn77/about_fts/docs/13673441/
- New formats (4.08/4.07), source of the last two:
  https://www.nalog.gov.ru/rn77/about_fts/docs/16493030/

When FNS publishes a new format version, add the XSD to `docs/` and update
this table.

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
