BUILD_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
DATA_DIR:=$(BUILD_DIR)/data

all: run

build:
	docker pull alephdata/memorious
	docker build -t alephdata/opensanctions .

run: build
	docker run -ti -v $(DATA_DIR):/data alephdata/opensanctions /bin/bash

data/osanc.entities:
	osanc-dump >data/osanc.entities

load: data/osanc.entities
	ftm-integrate load-entities -e data/osanc.entities

integration/recon:
	ftm-integrate dump-recon -r integration/recon

data/osanc.apply.entities: data/osanc.entities integration/recon
	cat data/osanc.entities | ftm apply-recon -r integration/recon >data/osanc.apply.entities

data/osanc.agg.entities: data/osanc.apply.entities
	cat data/osanc.apply.entities | ftm aggregate >data/osanc.agg.entities

integrate: data/osanc.agg.entities
