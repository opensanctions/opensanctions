URL="http://dfat.gov.au/international-relations/security/sanctions/Documents/regulation8_consolidated.xls"
AU_PATH=$(DATA_PATH)/au-sanctions
XLS_FILE=$(AU_PATH)/regulation8-consolidated-$(shell date +%Y%m%d).xls
CSV_FILE=$(AU_PATH)/regulation8-consolidated-$(shell date +%Y%m%d).csv

all: parse

$(XLS_FILE):
	mkdir -p $(AU_PATH)
	curl -o $(XLS_FILE) $(URL)

$(CSV_FILE): $(XLS_FILE)
	in2csv $(XLS_FILE) >$(CSV_FILE)

parse: $(CSV_FILE)

clean:
	rm $(XLS_FILE)
	rm $(CSV_FILE)

