URL="http://web.worldbank.org/external/default/main?contentMDK=64069844&menuPK=116730&pagePK=64148989&piPK=64148984&querycontentMDK=64069700&theSitePK=84266"
WB_PATH=$(DATA_PATH)/wb-debarred
HTML_FILE=$(WB_PATH)/list-$(shell date +%Y%m%d).html

all: parse

$(HTML_FILE):
	mkdir -p $(WB_PATH)
	curl -o $(HTML_FILE) $(URL)

parse: $(HTML_FILE)

clean:
	rm $(HTML_FILE)
