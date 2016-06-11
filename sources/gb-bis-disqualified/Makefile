WORK_PATH=$(OPENNAMES_SOURCE_DATA)
JSON_FILE=$(WORK_PATH)/gb-bis-disqualified.json

all: scrape parse

$(JSON_FILE):
	mkdir -p $(WORK_PATH)
	python scrape.py $(WORK_PATH) $(JSON_FILE)

scrape: $(JSON_FILE)

parse: $(JSON_FILE)

clean:
	rm $(JSON_FILE)
