
all: scrape parse

scrape: au-sanctions.scrape ch-seco-sanctions.scrape eu-eeas-sanctions.scrape \
	gb-hmt-sanctionslist.scrape int-every-politician.scrape int-interpol.scrape \
	int-wb-debarred.scrape ua-sdfm-blacklist.scrape un-sc-sanctions.scrape \
	us-bis-denied.scrape us-cia-world-leaders.scrape us-ofac.scrape

parse: au-sanctions.parse ch-seco-sanctions.parse eu-eeas-sanctions.parse \
	gb-hmt-sanctionslist.parse int-every-politician.parse int-interpol.parse \
	int-wb-debarred.parse ua-sdfm-blacklist.parse un-sc-sanctions.parse \
	us-bis-denied.parse us-cia-world-leaders.parse us-ofac.parse

%.scrape: env
	make -C $(basename $@) scrape

%.parse: env
	make -C $(basename $@) parse

env:
ifndef OPENNAMES_SOURCE_DATA
	$(error OPENNAMES_SOURCE_DATA is not set)
endif
ifndef OPENNAMES_JSON_DATA
	$(error OPENNAMES_JSON_DATA is not set)
endif
