BIS_PATH=$(DATA_PATH)/gb-bis-disqualified
CACHE_PATH=$(BIS_PATH)/gb-bis-disqualified/details
JSON_FILE=$(BIS_PATH)/disqualified-$(shell date +%Y%m%d).json

all: parse

$(JSON_FILE):
	mkdir -p $(CACHE_PATH)
	python scrape.py $(CACHE_PATH) $(JSON_FILE)

parse: $(JSON_FILE)

clean:
	rm $(JSON_FILE)
