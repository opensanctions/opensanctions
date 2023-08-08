all: clean fetch process publish

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/fragments.json:
	python parse.py

process: data/export/entities.ftm.json

publish:
	bash ../../upload.sh ru_egrul data/export

clean:
	rm -rf data/export data/fragments.json data/sorted.json
