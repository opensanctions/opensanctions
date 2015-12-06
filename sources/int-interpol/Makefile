IPOL_PATH=$(DATA_PATH)/int-interpol
JSON_FILE=$(IPOL_PATH)/wanted-$(shell date +%Y%m%d).json

all: parse

$(JSON_FILE):
	mkdir -p $(IPOL_PATH)
	python scrape.py $(JSON_FILE)

parse: $(JSON_FILE)
	pep parse interpol $(JSON_FILE)

clean:
	rm $(JSON_FILE)
