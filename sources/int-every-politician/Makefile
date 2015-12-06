EVPO_PATH=$(DATA_PATH)/int-every-politician
JSON_FILE=$(EVPO_PATH)/politicians-$(shell date +%Y%m%d).json

all: parse

$(JSON_FILE):
	mkdir -p $(EVPO_PATH)
	python scrape.py $(JSON_FILE)

parse: $(JSON_FILE)
	pep parse everypolitician $(JSON_FILE)

clean:
	rm $(JSON_FILE)
