URL=http://hmt-sanctions.s3.amazonaws.com/sanctionsconlist.xls
WORK_PATH=$(OPENNAMES_SOURCE_DATA)/gb-hmt-sanctionslist
XLS_FILE=$(WORK_PATH)/sanctionsconlist.xls
CSV_FILE=$(WORK_PATH)/sanctionsconlist.csv

all: scrape parse

$(XLS_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(XLS_FILE) $(URL)

$(CSV_FILE): $(XLS_FILE)
	in2csv $(XLS_FILE) >$(CSV_FILE)

scrape: $(CSV_FILE)

parse: $(CSV_FILE)
	pep parse hmt $(CSV_FILE)

clean:
	rm $(CSV_FILE)
