URL=http://www.sdfm.gov.ua/content/file/Site_docs/Black_list/zBlackListFull.xml
WORK_PATH=$(OPENNAMES_SOURCE_DATA)
XML_FILE=$(WORK_PATH)/ua-sdfm-blacklist.xml

all: scrape parse

$(XML_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(XML_FILE) $(URL)

scrape: $(XML_FILE)

parse: $(XML_FILE)
	python parse.py $(XML_FILE)

clean:
	rm $(XML_FILE)
