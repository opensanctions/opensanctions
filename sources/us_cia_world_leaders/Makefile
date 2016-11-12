WORK_PATH=$(OPENNAMES_SOURCE_DATA)
JSON_FILE=$(WORK_PATH)/us_cia_world_leaders.json

all: scrape parse

$(JSON_FILE):
	mkdir -p $(WORK_PATH)
	python scrape.py $(JSON_FILE)

scrape: $(JSON_FILE)

parse: $(JSON_FILE)
	python parse.py $(JSON_FILE)

clean:
	rm $(JSON_FILE)
