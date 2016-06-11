
all: extract

extract: scrape

scrape:
	make -C sources scrape

env:
ifndef OPENNAMES_SOURCE_DATA
	$(error OPENNAMES_SOURCE_DATA is not set)
endif
ifndef OPENNAMES_JSON_DATA
	$(error OPENNAMES_JSON_DATA is not set)
endif

