URL="http://web.worldbank.org/external/default/main?contentMDK=64069844&menuPK=116730&pagePK=64148989&piPK=64148984&querycontentMDK=64069700&theSitePK=84266"
WORK_PATH=$(OPENNAMES_SOURCE_DATA)
HTML_FILE=$(WORK_PATH)/int-wb-debarred.html

all: scrape parse

$(HTML_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(HTML_FILE) $(URL)

scrape: $(HTML_FILE)

parse: $(HTML_FILE)
	pep parse wbdeb $(HTML_FILE)

clean:
	rm $(HTML_FILE)
