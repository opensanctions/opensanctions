from json import loads
from datetime import datetime

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import clear_data_path
from zavod.exporters.nested import NestedTargetsJSONExporter
from zavod.exporters.nested import NestedTopicsJSONExporter
from zavod.crawl import crawl_dataset
from zavod.tests.exporters.util import harnessed_export


TIME_SECONDS_FMT = "%Y-%m-%dT%H:%M:%S"


def test_nested_targets(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(NestedTargetsJSONExporter, testdataset1)

    with open(dataset_path / "targets.nested.json") as nested_file:
        entities = [loads(line) for line in nested_file.readlines()]

    for entity in entities:
        # Fail if incorrect format
        datetime.strptime(entity["first_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_change"], TIME_SECONDS_FMT)
        assert entity["datasets"] == ["testdataset1"]

    john = [e for e in entities if e["id"] == "osv-john-doe"][0]
    assert john["properties"]["name"] == ["John Doe"]

    family_id = "osv-eb0a27f226377001807c04a1ca7de8502cf4d0cb"
    # Family relationship is not included as a root object
    assert len([e for e in entities if e["id"] == family_id]) == 0

    assert len(john["properties"]["familyPerson"]) == 1
    fam = john["properties"]["familyPerson"][0]
    assert fam["id"] == family_id
    assert fam["properties"]["person"][0] == "osv-john-doe"
    assert fam["properties"]["relative"][0]["id"] == "osv-jane-doe"


def test_nested_topics(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(NestedTopicsJSONExporter, testdataset1)

    with open(dataset_path / "topics.nested.json") as nested_file:
        entities = [loads(line) for line in nested_file.readlines()]

    assert len(entities) > 0, entities

    for entity in entities:
        # Fail if incorrect format
        datetime.strptime(entity["first_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_change"], TIME_SECONDS_FMT)
        assert entity["datasets"] == ["testdataset1"]
        topics = entity["properties"]["topics"]
        assert len(NestedTopicsJSONExporter._TOPICS.intersection(topics)) > 0

    john = [e for e in entities if e["id"] == "osv-mierscheid"][0]
    assert john["properties"]["name"] == ["Jakob Maria Mierscheid"]
