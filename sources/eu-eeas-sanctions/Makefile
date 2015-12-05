URL=http://ec.europa.eu/external_relations/cfsp/sanctions/list/version4/global/global.xml
EEAS_PATH=$(DATA_PATH)/eu-eeas-sanctions
XML_FILE=$(EEAS_PATH)/global-$(shell date +%Y%m%d).xml

all: parse

$(XML_FILE):
	mkdir -p $(EEAS_PATH)
	curl -o $(XML_FILE) $(URL)

parse: $(XML_FILE)

clean:
	rm $(XML_FILE)
