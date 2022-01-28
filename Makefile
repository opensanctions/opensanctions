DOCKER=

.PHONY: build

all: run

workdir:
	mkdir -p data/postgres

db:
	mkdir -p data/state
	curl -o data/state/opensanctions.sql.gz https://data.opensanctions.org/state/opensanctions.sql.gz
	gunzip -c data/state/opensanctions.sql.gz | psql $(OPENSANCTIONS_DATABASE_URI)

build:
	docker-compose build --pull

services:
	docker-compose up -d --remove-orphans db

shell: build workdir services
	docker-compose run --rm app bash

run: build workdir services
	docker-compose run --rm app opensanctions run

stop:
	docker-compose down --remove-orphans

clean:
	rm -rf data/datasets build dist .mypy_cache .pytest_cache
