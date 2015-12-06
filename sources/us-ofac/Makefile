OFAC_PATH=$(DATA_PATH)/us-ofac
SDN_FILE=$(OFAC_PATH)/sdn/$(shell date +%Y%m%d).xml
CONS_FILE=$(OFAC_PATH)/consolidated/$(shell date +%Y%m%d).xml

all: parse

$(SDN_FILE):
	mkdir -p $(OFAC_PATH)/sdn
	curl -o $(SDN_FILE) https://www.treasury.gov/ofac/downloads/sdn.xml

$(CONS_FILE):
	mkdir -p $(OFAC_PATH)/consolidated
	curl -o $(CONS_FILE) https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml

parse: $(SDN_FILE) $(CONS_FILE)
	pep-parse ofac --sdn $(SDN_FILE)
	pep-parse ofac --consolidated $(CONS_FILE)

clean:
	rm $(SDN_FILE)
	rm $(CONS_FILE)
