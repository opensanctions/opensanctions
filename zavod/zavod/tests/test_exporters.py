from csv import DictReader
from followthemoney.cli.util import path_entities
from followthemoney.proxy import EntityProxy
from json import load, loads
from nomenklatura.judgement import Judgement
from nomenklatura.stream import StreamEntity
from datetime import datetime

from zavod import settings
from zavod.context import Context
from zavod.dedupe import get_resolver
from zavod.exporters import export
from zavod.exporters.ftm import FtMExporter
from zavod.exporters.names import NamesExporter
from zavod.exporters.nested import NestedJSONExporter
from zavod.exporters.simplecsv import SimpleCSVExporter
from zavod.meta import Dataset, load_dataset_from_path
from zavod.runner import run_dataset
from zavod.store import View, get_store, get_view
from zavod.tests.conftest import DATASET_2_YML
from csv import DictReader

TIME_SECONDS_FMT = "%Y-%m-%dT%H:%M:%S"


def test_export(vdataset: Dataset):
    run_dataset(vdataset)
    export(vdataset.name)

    expected_resources = [
        "entities.ftm.json",
        "names.txt",
        "senzing.json",
        "source.csv",
        "statistics.json",
        "targets.nested.json",
        "targets.simple.csv",
    ]

    dataset_path = settings.DATA_PATH / "datasets" / vdataset.name

    # it parses and finds expected number of entites
    assert (
        len(list(path_entities(dataset_path / "entities.ftm.json", EntityProxy))) == 11
    )

    with open(dataset_path / "index.json") as index_file:
        index = load(index_file)
        assert index["name"] == vdataset.name
        assert index["entity_count"] == 11
        assert index["target_count"] == 8
        resources = {r["name"] for r in index["resources"]}
        for r in expected_resources:
            assert r in resources

    with open(dataset_path / "names.txt") as names_file:
        names = names_file.readlines()
        # it contains a couple of expected names
        assert "Jakob Maria Mierscheid\n" in names
        assert "Johnny Doe\n" in names

    with open(dataset_path / "resources.json") as resources_file:
        resources = {r["name"] for r in load(resources_file)["resources"]}
        for r in expected_resources:
            assert r in resources

    with open(dataset_path / "senzing.json") as senzing_file:
        targets = [loads(line) for line in senzing_file.readlines()]
        assert len(targets) == 8
        for target in targets:
            assert target["RECORD_TYPE"] in {"PERSON", "ORGANIZATION", "COMPANY"}

    with open(dataset_path / "statistics.json") as statistics_file:
        statistics = load(statistics_file)
        assert index["entity_count"] == 11
        assert index["target_count"] == 8

    with open(dataset_path / "targets.nested.json") as targets_nested_file:
        targets = [loads(r) for r in targets_nested_file.readlines()]
        assert len(targets) == 8
        for target in targets:
            assert target["schema"] in {"Person", "Organization", "Company"}

    with open(dataset_path / "targets.simple.csv") as targets_simple_file:
        targets = list(DictReader(targets_simple_file))
        assert len(targets) == 8
        assert "Johnny Doe" in {t["name"] for t in targets}


def harnessed_export(exporter_class, dataset) -> None:
    context = Context(dataset)
    context.begin(clear=False)
    store = get_store(dataset)
    view = store.view(dataset)

    exporter = exporter_class(context, view)
    exporter.setup()
    for entity in view.entities():
        exporter.feed(entity)
    exporter.finish()

    context.close()
    store.close()


def test_ftm(vdataset: Dataset):
    run_dataset(vdataset)
    harnessed_export(FtMExporter, vdataset)

    ftm_path = settings.DATA_PATH / "datasets" / vdataset.name / "entities.ftm.json"

    entities = list(path_entities(ftm_path, StreamEntity))

    for entity in entities:
        # Fail if incorrect format
        datetime.strptime(entity.first_seen, TIME_SECONDS_FMT)
        datetime.strptime(entity.last_seen, TIME_SECONDS_FMT)
        datetime.strptime(entity.last_change, TIME_SECONDS_FMT)
        assert entity.datasets == {"testdataset1"}

    john = [e for e in entities if e.id == "osv-john-doe"][0]
    john.get("name") == "John Doe"

    fam = [
        e for e in entities if e.id == "osv-eb0a27f226377001807c04a1ca7de8502cf4d0cb"
    ][0]
    assert fam.schema.name == "Family"


def test_ftm_referents(vdataset: Dataset):
    run_dataset(vdataset)

    resolver = get_resolver()
    identifier = resolver.decide(
        "osv-john-doe", "osv-johnny-does", Judgement.POSITIVE, user="test"
    )
    harnessed_export(FtMExporter, vdataset)

    ftm_path = settings.DATA_PATH / "datasets" / vdataset.name / "entities.ftm.json"
    entities = list(path_entities(ftm_path, EntityProxy))
    assert len(entities) == 11

    john = [e for e in entities if e.id == "osv-john-doe"][0]
    assert [identifier, "osv-johnny-does"] == sorted(john.to_dict()["referents"])

    johnny = [e for e in entities if e.id == "osv-johnny-does"][0]
    assert [identifier, "osv-john-doe"] == sorted(johnny.to_dict()["referents"])

    # Dedupe against an entity from another dataset.
    # The entity ID is included as referent but is not included in the export.

    dataset2 = load_dataset_from_path(DATASET_2_YML)
    run_dataset(dataset2)
    other_dataset_id = "td2-friedrich"

    resolver.decide("osv-john-doe", other_dataset_id, Judgement.POSITIVE, user="test")
    harnessed_export(FtMExporter, vdataset)

    entities = list(path_entities(ftm_path, EntityProxy))
    assert len(entities) == 11
    john = [e for e in entities if e.id == "osv-john-doe"][0]
    assert [identifier, "osv-johnny-does", other_dataset_id] == sorted(
        john.to_dict()["referents"]
    )
    assert [] == [e for e in entities if e.id == other_dataset_id]


def test_names(vdataset: Dataset):
    run_dataset(vdataset)
    harnessed_export(NamesExporter, vdataset)
    names_path = settings.DATA_PATH / "datasets" / vdataset.name / "names.txt"
    with open(names_path ) as names_file:
        names = names_file.readlines()

    # it contains a couple of expected names
    assert "Jakob Maria Mierscheid\n" in names
    assert "Johnny Doe\n" in names
    assert "Jane Doe\n" in names # Family member
    assert len(names) == 14


def test_nested(vdataset: Dataset):
    run_dataset(vdataset)
    harnessed_export(NestedJSONExporter, vdataset)

    nested_path = settings.DATA_PATH / "datasets" / vdataset.name / "targets.nested.json"
    with open(nested_path) as nested_file:
        entities = [loads(line) for line in nested_file.readlines()]

    for entity in entities:
        # Fail if incorrect format
        datetime.strptime(entity["first_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_change"], TIME_SECONDS_FMT)
        assert entity["datasets"] == ["testdataset1"]

    john = [e for e in entities if e["id"] == "osv-john-doe"][0]
    john.get("name") == "John Doe"

    family_id = "osv-eb0a27f226377001807c04a1ca7de8502cf4d0cb"
    # Family relationship is not included as a root object
    assert len([e for e in entities if e["id"] == family_id]) == 0

    assert len(john["properties"]["familyPerson"]) == 1
    fam = john["properties"]["familyPerson"][0]
    assert fam["id"] == family_id
    assert fam["properties"]["person"][0] == "osv-john-doe"
    assert fam["properties"]["relative"][0]["id"] == "osv-jane-doe"


def test_targets_simple(vdataset: Dataset):
    run_dataset(vdataset)
    harnessed_export(NestedJSONExporter, vdataset)

    csv_path = settings.DATA_PATH / "datasets" / vdataset.name / "targets.simple.csv"
    with open(csv_path) as csv_file:
        reader = DictReader(csv_file)
        rows = list(reader)
    
    john = [r for r in rows if r["id"] == "osv-john-doe"][0]
    # Some people probably assume column order even though they ideally shouldn't
    assert list(john.keys()) == [
        "id",
        "schema",
        "name",
        "aliases",
        "birth_date",
        "countries",
        "addresses",
        "identifiers",
        "sanctions",
        "phones",
        "emails",
        "dataset",
        "first_seen",
        "last_seen",
        "last_change",
    ]
    assert john == {
        "id": "osv-john-doe",
        "schema": "Person",
        "name": "John Doe",
        "aliases": "",
        "birth_date": "1975",
        "countries": "us",
        "addresses": "",
        "identifiers": "",
        "sanctions": "",
        "phones": "",
        "emails": "",
        "dataset": "OpenSanctions Validation Dataset",  # Dataset title
        "first_seen": settings.RUN_TIME_ISO, # Seconds string format
        "last_seen": settings.RUN_TIME_ISO,
        "last_change": settings.RUN_TIME_ISO,
    }
    # Assert the dates above are in the expected format
    datetime.strptime(settings.RUN_TIME_ISO, TIME_SECONDS_FMT)