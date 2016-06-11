URL=https://www.un.org/sc/suborg/sites/www.un.org.sc.suborg/files/consolidated.xml
WORK_PATH=$(OPENNAMES_SOURCE_DATA)
XML_FILE=$(WORK_PATH)/un-sc-sanctions-consolidated.xml

all: scrape parse

$(XML_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(XML_FILE) $(URL)

scrape: $(XML_FILE)

parse: $(XML_FILE)
	pep parse unsc $(XML_FILE)

clean:
	rm $(XML_FILE)

