URL=http://ec.europa.eu/external_relations/cfsp/sanctions/list/version4/global/global.xml
WORK_PATH=$(OPENNAMES_SOURCE_DATA)
XML_FILE=$(WORK_PATH)/eu_eeas_sanctions.xml

all: scrape parse

$(XML_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(XML_FILE) $(URL)

scrape: $(XML_FILE)

parse: $(XML_FILE)
	python parse.py $(XML_FILE)

clean:
	rm $(XML_FILE)
