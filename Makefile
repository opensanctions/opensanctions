DOCKER=
ALEMBIC=alembic -c opensanctions/migrate/alembic.ini

all: run

workdir:
	mkdir -p data/postgres

build:
	docker compose build --pull

shell: build workdir
	mkdir -p $(PWD)/data
	docker compose run --rm app bash

run: build workdir
	docker compose run --rm app opensanctions run

stop:
	docker compose down --remove-orphans