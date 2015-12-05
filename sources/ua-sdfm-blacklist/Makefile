URL=http://www.sdfm.gov.ua/content/file/Site_docs/Black_list/zBlackListFull.xml
SDFM_PATH=$(DATA_PATH)/ua-sdfm-blacklist
XML_FILE=$(SDFM_PATH)/blacklist-$(shell date +%Y%m%d).xml

all: parse

$(XML_FILE):
	mkdir -p $(SDFM_PATH)
	curl -o $(XML_FILE) $(URL)

parse: $(XML_FILE)

clean:
	rm $(XML_FILE)
