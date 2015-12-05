CIA_PATH=$(DATA_PATH)/us-cia-world-leaders
JSON_FILE=$(CIA_PATH)/world-leaders-$(shell date +%Y%m%d).json

all: parse

$(JSON_FILE):
	mkdir -p $(CIA_PATH)
	python scrape.py $(JSON_FILE)

parse: $(JSON_FILE)

clean:
	rm $(JSON_FILE)
