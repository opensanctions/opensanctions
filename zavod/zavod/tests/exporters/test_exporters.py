from csv import DictReader
from followthemoney.cli.util import path_entities
from followthemoney.proxy import EntityProxy
from json import load, loads
from nomenklatura.judgement import Judgement
from nomenklatura.stream import StreamEntity
from nomenklatura.statement import Statement, CSV
from nomenklatura.statement.serialize import read_path_statements
from datetime import datetime

from zavod import settings
from zavod.store import get_view
from zavod.dedupe import get_resolver
from zavod.exporters import export_dataset
from zavod.archive import clear_data_path
from zavod.exporters.ftm import FtMExporter
from zavod.exporters.names import NamesExporter
from zavod.exporters.nested import NestedJSONExporter
from zavod.exporters.simplecsv import SimpleCSVExporter
from zavod.exporters.senzing import SenzingExporter
from zavod.exporters.statements import StatementsCSVExporter
from zavod.meta import Dataset, load_dataset_from_path
from zavod.crawl import crawl_dataset
from zavod.tests.conftest import DATASET_2_YML, COLLECTION_YML
from zavod.tests.exporters.util import harnessed_export


TIME_SECONDS_FMT = "%Y-%m-%dT%H:%M:%S"

always_exports = {"statistics.json"}
default_exports = {
    "entities.ftm.json",
    "names.txt",
    "senzing.json",
    "source.csv",
    "targets.nested.json",
    "targets.simple.csv",
}


def export(dataset: Dataset) -> None:
    view = get_view(dataset)
    export_dataset(dataset, view)


def test_export(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    export(testdataset1)

    # it parses and finds expected number of entities
    assert (
        len(list(path_entities(dataset_path / "entities.ftm.json", EntityProxy))) == 11
    )

    with open(dataset_path / "index.json") as index_file:
        index = load(index_file)
        assert index["name"] == testdataset1.name
        assert index["entity_count"] == 11
        assert index["target_count"] == 7
        resources = {r["name"] for r in index["resources"]}
        for r in set.union(default_exports, always_exports):
            assert r in resources

    with open(dataset_path / "names.txt") as names_file:
        names = names_file.readlines()
        # it contains a couple of expected names
        assert "Jakob Maria Mierscheid\n" in names
        assert "Johnny Doe\n" in names

    with open(dataset_path / "resources.json") as resources_file:
        resources = {r["name"] for r in load(resources_file)["resources"]}
        for r in set.union(default_exports, always_exports):
            assert r in resources

    with open(dataset_path / "senzing.json") as senzing_file:
        entities = [loads(line) for line in senzing_file.readlines()]
        assert len(entities) == 8
        for ent in entities:
            assert ent["RECORD_TYPE"] in {"PERSON", "ORGANIZATION"}

    with open(dataset_path / "statistics.json") as statistics_file:
        statistics = load(statistics_file)
        assert statistics["entity_count"] == 11
        assert statistics["target_count"] == 7

    with open(dataset_path / "targets.nested.json") as targets_nested_file:
        targets = [loads(r) for r in targets_nested_file.readlines()]
        assert len(targets) == 7
        for target in targets:
            assert target["schema"] in {"Person", "Organization", "Company"}

    with open(dataset_path / "targets.simple.csv") as targets_simple_file:
        targets = list(DictReader(targets_simple_file))
        assert len(targets) == 7
        assert "Oswell E. Spencer" in {t["name"] for t in targets}


def test_minimal_export_config(testdataset2: Dataset):
    """Test export when dataset.exporters is empty list"""
    dataset_path = settings.DATA_PATH / "datasets" / testdataset2.name
    clear_data_path(testdataset2.name)

    crawl_dataset(testdataset2)
    export(testdataset2)

    with open(dataset_path / "index.json") as index_file:
        index = load(index_file)
        resources = {r["name"] for r in index["resources"]}
        for r in always_exports:
            assert r in resources
        for r in default_exports:
            assert r not in resources

    with open(dataset_path / "resources.json") as resources_file:
        resources = {r["name"] for r in load(resources_file)["resources"]}
        for r in always_exports:
            assert r in resources
        for r in default_exports:
            assert r not in resources


def test_custom_export_config(testdataset2_export: Dataset):
    """Test export when dataset.exporters has custom exports listed"""
    dataset_path = settings.DATA_PATH / "datasets" / testdataset2_export.name
    clear_data_path(testdataset2_export.name)

    crawl_dataset(testdataset2_export)
    export(testdataset2_export)

    with open(dataset_path / "index.json") as index_file:
        index = load(index_file)
        resources = {r["name"] for r in index["resources"]}
        for r in set.union(always_exports, {"names.txt", "pep-positions.json"}):
            assert r in resources
        for r in default_exports - {"names.txt", "pep-positions.json"}:
            assert r not in resources

    with open(dataset_path / "resources.json") as resources_file:
        resources = {r["name"] for r in load(resources_file)["resources"]}
        for r in set.union(always_exports, {"names.txt", "pep-positions.json"}):
            assert r in resources
        for r in default_exports - {"names.txt", "pep-positions.json"}:
            assert r not in resources


def test_ftm(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(FtMExporter, testdataset1)

    entities = list(path_entities(dataset_path / "entities.ftm.json", StreamEntity))
    for entity in entities:
        # Fail if incorrect format
        assert entity.first_seen is not None
        datetime.strptime(entity.first_seen, TIME_SECONDS_FMT)
        assert entity.last_seen is not None
        datetime.strptime(entity.last_seen, TIME_SECONDS_FMT)
        assert entity.last_change is not None
        datetime.strptime(entity.last_change, TIME_SECONDS_FMT)
        assert entity.datasets == {"testdataset1"}

    john = [e for e in entities if e.id == "osv-john-doe"][0]
    assert john.get("name") == ["John Doe"]

    fam = [
        e for e in entities if e.id == "osv-eb0a27f226377001807c04a1ca7de8502cf4d0cb"
    ][0]
    assert fam.schema.name == "Family"


def test_ftm_referents(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    resolver = get_resolver()
    identifier = resolver.decide(
        "osv-john-doe", "osv-johnny-does", Judgement.POSITIVE, user="test"
    )
    crawl_dataset(testdataset1)
    harnessed_export(FtMExporter, testdataset1)

    entities = list(path_entities(dataset_path / "entities.ftm.json", EntityProxy))
    assert len(entities) == 10

    john = [e for e in entities if e.id == identifier][0]
    john_dict = john.to_dict()
    assert len(john_dict["referents"]) == 2
    assert "osv-johnny-does" in john_dict["referents"]
    assert "osv-john-doe" in john_dict["referents"]
    assert str(identifier) not in john_dict["referents"]
    assert len(john_dict["datasets"]) == 1
    assert "testdataset1" in john_dict["datasets"]

    # Dedupe against an entity from another dataset.
    # The entity ID is included as referent but is not included in the export.

    dataset2 = load_dataset_from_path(DATASET_2_YML)
    assert dataset2 is not None
    collection = load_dataset_from_path(COLLECTION_YML)
    assert collection is not None
    collection_path = settings.DATA_PATH / "datasets" / collection.name
    crawl_dataset(dataset2)
    other_dataset_id = "td2-freddie-bloggs"
    harnessed_export(FtMExporter, collection)
    entities = list(path_entities(collection_path / "entities.ftm.json", EntityProxy))
    assert len(entities) == 28

    resolver.decide("osv-john-doe", other_dataset_id, Judgement.POSITIVE, user="test")
    clear_data_path(collection.name)
    harnessed_export(FtMExporter, collection)
    entities = list(path_entities(collection_path / "entities.ftm.json", EntityProxy))
    assert len(entities) == 27  # After deduplication there's one less entity
    assert [] == [e for e in entities if e.id == other_dataset_id]

    john = [e for e in entities if e.id == identifier][0]
    john_dict = john.to_dict()
    assert "osv-johnny-does" in john_dict["referents"]
    assert "osv-john-doe" in john_dict["referents"]
    assert other_dataset_id in john_dict["referents"]
    assert len(john_dict["datasets"]) == 2
    assert collection.name not in john_dict["datasets"]


def test_names(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(NamesExporter, testdataset1)

    with open(dataset_path / "names.txt") as names_file:
        names = names_file.readlines()

    # it contains a couple of expected names
    assert "Jakob Maria Mierscheid\n" in names
    assert "Johnny Doe\n" in names
    assert "Jane Doe\n" in names  # Family member
    assert len(names) == 14


def test_nested(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(NestedJSONExporter, testdataset1)

    with open(dataset_path / "targets.nested.json") as nested_file:
        entities = [loads(line) for line in nested_file.readlines()]

    for entity in entities:
        # Fail if incorrect format
        datetime.strptime(entity["first_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_seen"], TIME_SECONDS_FMT)
        datetime.strptime(entity["last_change"], TIME_SECONDS_FMT)
        assert entity["datasets"] == ["testdataset1"]

    john = [e for e in entities if e["id"] == "osv-john-doe"][0]
    assert john['properties']["name"] == ["John Doe"]

    family_id = "osv-eb0a27f226377001807c04a1ca7de8502cf4d0cb"
    # Family relationship is not included as a root object
    assert len([e for e in entities if e["id"] == family_id]) == 0

    assert len(john["properties"]["familyPerson"]) == 1
    fam = john["properties"]["familyPerson"][0]
    assert fam["id"] == family_id
    assert fam["properties"]["person"][0] == "osv-john-doe"
    assert fam["properties"]["relative"][0]["id"] == "osv-jane-doe"


def test_targets_simple(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(SimpleCSVExporter, testdataset1)

    with open(dataset_path / "targets.simple.csv") as csv_file:
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
        "first_seen": settings.RUN_TIME_ISO,  # Seconds string format
        "last_seen": settings.RUN_TIME_ISO,
        "last_change": settings.RUN_TIME_ISO,
    }
    # Assert the dates above are in the expected format
    datetime.strptime(settings.RUN_TIME_ISO, TIME_SECONDS_FMT)


def test_senzing(testdataset1: Dataset):
    """Tests whether the senzing output contain the expected entities, with expected
    keys and value formats."""
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(SenzingExporter, testdataset1)

    with open(dataset_path / "senzing.json") as senzing_file:
        targets = [loads(line) for line in senzing_file.readlines()]
    company = [t for t in targets if t["RECORD_ID"] == "osv-umbrella-corp"][0]
    company_features = company.pop("FEATURES")

    assert {
        "NAME_TYPE": "PRIMARY",
        "NAME_ORG": "Umbrella Corporation",
    } in company_features
    assert {
        "NAME_TYPE": "ALIAS",
        "NAME_ORG": "Umbrella Pharmaceuticals, Inc.",
    } in company_features
    assert {"REGISTRATION_DATE": "1980"} in company_features
    assert {"REGISTRATION_COUNTRY": "us"} in company_features
    assert {"NATIONAL_ID_NUMBER": "8723-BX"} in company_features
    assert company == {
        "DATA_SOURCE": "OS_TESTDATASET1",
        "RECORD_ID": "osv-umbrella-corp",
        "RECORD_TYPE": "ORGANIZATION",
    }

    person = [t for t in targets if t["RECORD_ID"] == "osv-hans-gruber"][0]
    person_features = person.pop("FEATURES")
    assert {"NAME_TYPE": "PRIMARY", "NAME_FULL": "Hans Gruber"} in person_features
    assert {"NAME_TYPE": "ALIAS", "NAME_FULL": "Bill Clay"} in person_features
    assert {"ADDR_FULL": "Lauensteiner Str. 49, 01277 Dresden"} in person_features
    assert {"DATE_OF_BIRTH": "1978-09-25"} in person_features
    assert {"NATIONALITY": "dd"} in person_features
    assert person == {
        "DATA_SOURCE": "OS_TESTDATASET1",
        "RECORD_ID": "osv-hans-gruber",
        "RECORD_TYPE": "PERSON",
    }


def test_statements(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(StatementsCSVExporter, testdataset1)

    path = dataset_path / "statements.csv"
    statements = list(read_path_statements(path, CSV, Statement))
    entities = [s.canonical_id for s in statements if s.prop == Statement.BASE]
    assert len(entities) == 11
