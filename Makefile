all: shell

build:
	docker-compose build

services:
	docker-compose up -d postgres redis
	sleep 3

shell: build services
	docker-compose run worker sh
