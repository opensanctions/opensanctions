URL=http://hmt-sanctions.s3.amazonaws.com/sanctionsconlist.xls
WORK_PATH=$(OPENNAMES_SOURCE_DATA)
XLS_FILE=$(WORK_PATH)/gb_hmt_sanctionslist.xls
CSV_FILE=$(WORK_PATH)/gb_hmt_sanctionslist.csv

all: scrape parse

$(XLS_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(XLS_FILE) $(URL)

$(CSV_FILE): $(XLS_FILE)
	in2csv $(XLS_FILE) >$(CSV_FILE)

scrape: $(CSV_FILE)

parse: $(CSV_FILE)
	python parse.py $(CSV_FILE)

clean:
	rm $(CSV_FILE)
