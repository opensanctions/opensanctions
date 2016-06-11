
all: au-sanctions ch-seco-sanctions eu-eeas-sanctions gb-hmt-sanctionslist \
	int-every-politician int-interpol ua-sdfm-blacklist un-sc-sanctions \
	us-bis-denied us-cia-world-leaders us-ofac wb-debarred

au-sanctions: env
	make -C sources/au-sanctions

ch-seco-sanctions: env
	make -C sources/ch-seco-sanctions

eu-eeas-sanctions: env
	make -C sources/eu-eeas-sanctions

gb-hmt-sanctionslist: env
	make -C sources/gb-hmt-sanctionslist

int-every-politician: env
	make -C sources/int-every-politician

int-interpol: env
	make -C sources/int-interpol

int-wb-debarred: env
	make -C sources/int-wb-debarred

ua-sdfm-blacklist: env
	make -C sources/ua-sdfm-blacklist

un-sc-sanctions: env
	make -C sources/un-sc-sanctions

us-bis-denied: env
	make -C sources/us-bis-denied

us-cia-world-leaders: env
	make -C sources/us-cia-world-leaders

us-ofac: env
	make -C sources/us-ofac

env:
ifndef DATA_PATH
	$(error DATA_PATH is not set)
endif
