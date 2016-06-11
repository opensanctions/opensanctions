WORK_PATH=$(OPENNAMES_SOURCE_DATA)/int-every-politician
JSON_FILE=$(WORK_PATH)/politicians.json

all: scrape parse

$(JSON_FILE):
	mkdir -p $(WORK_PATH)
	python scrape.py $(JSON_FILE)

scrape: $(JSON_FILE)

parse: $(JSON_FILE)
	pep parse everypolitician $(JSON_FILE)

clean:
	rm $(JSON_FILE)
