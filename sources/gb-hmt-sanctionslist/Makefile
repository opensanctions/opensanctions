URL=http://hmt-sanctions.s3.amazonaws.com/sanctionsconlist.xls
HMT_PATH=$(DATA_PATH)/gb-hmt-sanctionslist
XLS_FILE=$(HMT_PATH)/sanctionsconlist-$(shell date +%Y%m%d).xls
CSV_FILE=$(HMT_PATH)/sanctionsconlist-$(shell date +%Y%m%d).csv

all: parse

$(XLS_FILE):
	mkdir -p $(HMT_PATH)
	curl -o $(XLS_FILE) $(URL)

$(CSV_FILE): $(XLS_FILE)
	in2csv $(XLS_FILE) >$(CSV_FILE)

parse: $(CSV_FILE)
	pep parse hmt $(CSV_FILE)

clean:
	rm $(CSV_FILE)
