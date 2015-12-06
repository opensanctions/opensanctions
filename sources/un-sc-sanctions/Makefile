URL=https://www.un.org/sc/suborg/sites/www.un.org.sc.suborg/files/consolidated.xml
UNSC_PATH=$(DATA_PATH)/un-sc-sanctions
XML_FILE=$(UNSC_PATH)/consolidated-$(shell date +%Y%m%d).xml

all: parse

$(XML_FILE):
	mkdir -p $(UNSC_PATH)
	curl -o $(XML_FILE) $(URL)

parse: $(XML_FILE)
	pep parse unsc $(XML_FILE)

clean:
	rm $(XML_FILE)

