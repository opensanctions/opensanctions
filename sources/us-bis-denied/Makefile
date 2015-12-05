URL=https://www.bis.doc.gov/dpl/dpl.txt
BIS_PATH=$(DATA_PATH)/us-bis-denied
TXT_FILE=$(BIS_PATH)/denied-$(shell date +%Y%m%d).csv

all: parse

$(TXT_FILE):
	mkdir -p $(BIS_PATH)
	curl -o $(TXT_FILE) $(URL)

parse: $(TXT_FILE)

clean:
	rm $(TXT_FILE)
