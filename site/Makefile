
data: _data/index.json _data/issues.json _data/targets.ijson

_data:
	mkdir -p _data/

_data/index.json: _data
	curl -s -o _data/index.json https://data.opensanctions.org/datasets/latest/index.json

_data/issues.json: _data
	curl -s -o _data/issues.json https://data.opensanctions.org/datasets/latest/issues.json

_data/targets.ijson: _data
	curl -s -o _data/targets.ijson https://data.opensanctions.org/datasets/latest/default/targets.nested.json

clean:
	rm -rf out _data