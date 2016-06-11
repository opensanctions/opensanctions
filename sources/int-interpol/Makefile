WORK_PATH=$(OPENNAMES_SOURCE_DATA)/int-interpol
JSON_FILE=$(WORK_PATH)/wanted.json

all: scrape parse

$(JSON_FILE):
	mkdir -p $(WORK_PATH)
	python scrape.py $(JSON_FILE)

scrape: $(JSON_FILE)

parse: $(JSON_FILE)
	pep parse interpol $(JSON_FILE)

clean:
	rm $(JSON_FILE)
