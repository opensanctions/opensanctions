import os
import csv
import json
from pathlib import Path
from datetime import datetime

DATE_FMT = "%Y-%m-%dT%H:%M:%S"
ADDR_TEXT = "Lauensteiner Str. 49, 01277 Dresden"
ENTITY_COUNT = 11
TARGET_COUNT = 7


def validation_path(file_name: str):
    data_path = os.environ.get("OPENSANCTIONS_DATA_PATH")
    assert data_path is not None
    validation_data = Path(data_path) / "datasets/validation"
    return validation_data / file_name


def test_entities_json():
    last_seen_dataset = None
    ids = set()
    targets = 0
    with open(validation_path("entities.ftm.json"), "r") as fh:
        while line := fh.readline():
            data = json.loads(line)
            entity_id = data["id"]
            target = data["target"]
            assert isinstance(target, bool)
            if target:
                targets += 1
            assert len(data["datasets"]) == 1
            schema = data["schema"]
            assert len(entity_id) > 3, data
            ids.add(entity_id)
            assert len(schema), data
            props = data["properties"]
            first_seen = datetime.strptime(data["first_seen"], DATE_FMT)
            last_seen = datetime.strptime(data["last_seen"], DATE_FMT)
            if last_seen_dataset is None:
                last_seen_dataset = last_seen
            else:
                assert last_seen_dataset == last_seen
            assert first_seen <= last_seen
            assert first_seen <= datetime.utcnow()
            assert isinstance(props, dict)
            if entity_id == "NK-JsVssJtnUWyf2Seb3yzYHN":
                assert schema == "Person"
                assert data["caption"] == "Johnny Doe"
                assert "1975" in props["birthDate"]
                assert len(props["birthDate"]) == 1
                assert len(props["nationality"]) == 2
                assert len(data["referents"]) == 2
            elif entity_id.startswith("addr-"):
                assert data["caption"] == ADDR_TEXT
            elif schema == "Family":
                assert len(props["person"]) == 1
                assert len(props["relative"]) == 1
            elif schema == "Person":
                assert entity_id.startswith("osv-")
                # assert False, data
                assert len(data["referents"]) == 0

    assert "NK-JsVssJtnUWyf2Seb3yzYHN" in ids
    assert "osv-hans-gruber" in ids
    assert len(ids) == ENTITY_COUNT
    assert targets == TARGET_COUNT


def test_targets_nested_json():
    targets = 0
    with open(validation_path("targets.nested.json"), "r") as fh:
        while line := fh.readline():
            data = json.loads(line)
            assert data["id"]
            props = data["properties"]
            if data["id"] == "NK-JsVssJtnUWyf2Seb3yzYHN":
                fp = props["familyPerson"]
                assert len(fp) == 1
                rel = fp[0]
                assert rel["target"] == False
                assert "first_seen" in rel
                assert len(rel["datasets"]) == 1
                assert len(rel["referents"]) == 0
                relative = rel["properties"]["relative"][0]
                assert relative["id"] == "osv-jane-doe"
                # assert False, list(props.keys())
            if data["id"] == "osv-john-gruber":
                addrs = props["addressEntity"]
                assert len(addrs) == 1
                assert addrs[0]["caption"] == ADDR_TEXT
                assert addrs[0]["properties"]["full"][0] == ADDR_TEXT
                # assert False, list(props.keys())
            targets += 1
    assert targets == TARGET_COUNT, fh


def test_targets_simple_csv():
    targets = 0
    with open(validation_path("targets.simple.csv"), "r") as fh:
        for row in csv.DictReader(fh):
            targets += 1
            assert "birth_date" in row
            assert "countries" in row
    assert targets == TARGET_COUNT, fh


def test_names_txt():
    with open(validation_path("names.txt"), "r") as fh:
        names = [n.strip() for n in fh.readlines()]
        assert len(names) == len(set(names))
        assert len(names) > ENTITY_COUNT
        assert "John Doe" in names
        assert "Umbrella Corporation" in names


def test_index_json():
    with open(validation_path("index.json"), "r") as fh:
        data = json.load(fh)
        assert data["name"] == "validation"
        assert data["hidden"] == True
        assert len(data["resources"]) == 6
        print(list(data.keys()))
        assert datetime.strptime(data["last_change"], DATE_FMT)
        assert data["targets"]["total"] == TARGET_COUNT
