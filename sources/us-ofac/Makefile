WORK_PATH=$(OPENNAMES_SOURCE_DATA)/us-ofac
SDN_FILE=$(WORK_PATH)/sdn.xml
CONS_FILE=$(WORK_PATH)/consolidated.xml

all: scrape parse

$(SDN_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(SDN_FILE) https://www.treasury.gov/ofac/downloads/sdn.xml

$(CONS_FILE):
	mkdir -p $(WORK_PATH)
	curl -o $(CONS_FILE) https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml

scrape: $(SDN_FILE) $(CONS_FILE)

parse: $(SDN_FILE) $(CONS_FILE)
	pep parse ofac --sdn $(SDN_FILE)
	pep parse ofac --consolidated $(CONS_FILE)

clean:
	rm $(SDN_FILE)
	rm $(CONS_FILE)
