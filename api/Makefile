IMAGE=pudo/opensanctions-api:latest

run: build
	docker run -ti $(IMAGE) bash 

build:
	docker build -t $(IMAGE) .

api: build
	docker run -ti -p 8000:8000 $(IMAGE)