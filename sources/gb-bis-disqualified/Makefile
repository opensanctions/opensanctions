WORK_PATH=$(OPENNAMES_SOURCE_DATA)/gb-bis-disqualified
JSON_FILE=$(WORK_PATH)/disqualified.json

all: scrape parse

$(JSON_FILE):
	mkdir -p $(WORK_PATH)
	python scrape.py $(WORK_PATH) $(JSON_FILE)

scrape: $(JSON_FILE)

parse: $(JSON_FILE)

clean:
	rm $(JSON_FILE)
