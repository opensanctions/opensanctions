SHELL := /bin/bash
# Check if 'docker-compose' is available, if not, use 'docker compose'
COMPOSE_CMD := $(shell command -v docker-compose >/dev/null 2>&1 && echo "docker-compose" || (docker compose version >/dev/null 2>&1 && echo "docker compose"))

TS=$(shell date +%Y%m%d%H%M)

.PHONY: build

all: run

workdir:
	mkdir -p data/postgres

build:
	$(COMPOSE_CMD) build --pull

services:
	$(COMPOSE_CMD) up -d --remove-orphans db

shell: build workdir services
	$(COMPOSE_CMD) run --rm app bash

run: build workdir services
	$(COMPOSE_CMD) run --rm app opensanctions run

stop:
	$(COMPOSE_CMD) down --remove-orphans

clean:
	rm -rf data/datasets build dist .mypy_cache .pytest_cache
