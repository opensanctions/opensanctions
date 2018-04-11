
build:
	docker build -t alephdata/opensanctions .

run: build
	docker run -ti alephdata/opensanctions /bin/bash