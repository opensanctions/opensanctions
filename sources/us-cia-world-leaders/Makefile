WORK_PATH=$(OPENNAMES_SOURCE_DATA)/us-cia-world-leaders
JSON_FILE=$(WORK_PATH)/world-leaders-$(shell date +%Y%m%d).json

all: scrape parse

$(JSON_FILE):
	mkdir -p $(WORK_PATH)
	python scrape.py $(JSON_FILE)

scrape: $(JSON_FILE)

parse: $(JSON_FILE)
	pep parse worldleaders $(JSON_FILE)

clean:
	rm $(JSON_FILE)
