URL="https://www.sesam.search.admin.ch/sesam-search-web/pages/downloadXmlGesamtliste.xhtml?lang=en&action=downloadXmlGesamtlisteAction"
WORK_PATH=$(OPENNAMES_SOURCE_DATA)
XML_FILE=$(WORK_PATH)/ch_seco_sanctions.xml

all: scrape parse

$(XML_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(XML_FILE) $(URL)

scrape: $(XML_FILE)

parse: $(XML_FILE)
	python parse.py $(XML_FILE)

clean:
	rm $(XML_FILE)
