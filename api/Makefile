IMAGE=pudo/opensanctions-api:latest

all:
	make index
	make api

run: build
	docker compose run --rm app bash

build:
	docker compose build --pull

services:
	docker compose up --remove-orphans --wait -d index

api: build services
	docker compose up --remove-orphans app

index: build services
	docker compose run --rm app python3 osapi/index.py