URL="http://dfat.gov.au/international-relations/security/sanctions/Documents/regulation8_consolidated.xls"
WORK_PATH=$(OPENNAMES_SOURCE_DATA)
XLS_FILE=$(WORK_PATH)/au_sanctions_regulation8_consolidated.xls
CSV_FILE=$(WORK_PATH)/au_sanctions_regulation8_consolidated.csv

all: scrape parse

$(XLS_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(XLS_FILE) $(URL)

$(CSV_FILE): $(XLS_FILE)
	in2csv $(XLS_FILE) >$(CSV_FILE)

scrape: $(CSV_FILE)

parse: $(CSV_FILE)

clean:
	rm $(XLS_FILE)
	rm $(CSV_FILE)

