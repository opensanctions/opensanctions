
all: scrape parse

scrape:
	make -C sources scrape

parse:
	make -C sources parse

env:
ifndef OPENNAMES_SOURCE_DATA
	$(error OPENNAMES_SOURCE_DATA is not set)
endif
ifndef OPENNAMES_JSON_DATA
	$(error OPENNAMES_JSON_DATA is not set)
endif

