DOCKER=
ALEMBIC=alembic -c opensanctions/migrate/alembic.ini

all: run

workdir:
	mkdir -p data/postgres

build: workdir
	docker compose build --pull

shell: build
	mkdir -p $(PWD)/data
	docker compose run --rm app bash

run: build
	docker compose run --rm app opensanctions run
