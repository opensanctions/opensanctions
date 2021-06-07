DOCKER=
ALEMBIC=alembic -c opensanctions/migrate/alembic.ini

all: run

build:
	docker build --pull -t opensanctions .

shell: build
	mkdir -p $(PWD)/data
	docker run -ti --rm -v $(PWD)/data:/data opensanctions bash

run: build
	docker run -ti --rm -v $(PWD)/data:/data opensanctions
