URL="https://www.sesam.search.admin.ch/sesam-search-web/pages/downloadXmlGesamtliste.xhtml?lang=en&action=downloadXmlGesamtlisteAction"
SECO_PATH=$(DATA_PATH)/ch-seco-sanctions
XML_FILE=$(SECO_PATH)/gesamt-$(shell date +%Y%m%d).xml

all: parse

$(XML_FILE):
	mkdir -p $(SECO_PATH)
	curl -o $(XML_FILE) $(URL)

parse: $(XML_FILE)

clean:
	rm $(XML_FILE)
