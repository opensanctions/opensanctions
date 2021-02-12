all: shell

build:
	docker-compose build

shell:
	docker-compose run worker bash

run: 
	docker-compose run worker