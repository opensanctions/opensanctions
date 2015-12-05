URL=http://hmt-sanctions.s3.amazonaws.com/sanctionsconlist.csv
HMT_PATH=$(DATA_PATH)/gb-hmt-sanctionslist
CSV_FILE=$(HMT_PATH)/sanctionsconlist-$(shell date +%Y%m%d).csv

all: parse

$(CSV_FILE):
	mkdir -p $(HMT_PATH)
	curl -o $(CSV_FILE) $(URL)

parse: $(CSV_FILE)

clean:
	rm $(CSV_FILE)
