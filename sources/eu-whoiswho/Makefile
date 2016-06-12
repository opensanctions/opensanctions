WORK_PATH=$(OPENNAMES_SOURCE_DATA)
XML_FILE=$(WORK_PATH)/eu-whoiswho.json

all: scrape parse

$(XML_FILE):
	python scrape.py $(XML_FILE)

scrape: $(XML_FILE)

parse: $(XML_FILE)
	python parse.py $(XML_FILE)

clean:
	rm $(XML_FILE)
