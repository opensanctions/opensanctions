data:
	mkdir -p data

data/peps.json: data
	wget -O data/peps.json https://data.opensanctions.org/datasets/latest/peps/entities.ftm.json

data/default.json: data
	wget -O data/default.json https://data.opensanctions.org/datasets/latest/default/entities.ftm.json
