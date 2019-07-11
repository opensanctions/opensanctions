all: shell

build:
	docker-compose build

shell: build
	docker-compose run worker sh
