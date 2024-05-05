TS=$(shell date +%Y%m%d)

crawl:
	mkdir -p hetq-data
	python download.py tmp

zip:
	mkdir -p tmp
	zip -r tmp/hetq-data.zip hetq-data

upload: tmp/hetq-data.zip
	gsutil cp tmp/hetq-data.zip "gs://internal-data.opensanctions.org/am_hetq_peps/$(TS)/"
