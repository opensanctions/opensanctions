from zavod.exporters import export
from zavod.context import Context
from zavod.store import View, get_store, get_view
from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod import settings
from followthemoney.cli.util import read_entities
from json import load, loads
from csv import DictReader
from zavod.exporters.ftm import FtMExporter


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
    with open(dataset_path / "entities.ftm.json") as ftm:
        # it parses and finds expected number of entites
        assert len(list(read_entities(ftm))) == 11

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


def test_ftm(vdataset: Dataset):
    run_dataset(vdataset)
    context = Context(vdataset)
    context.begin(clear=False)
    store = get_store(vdataset)
    view = store.view(vdataset)

    exporter = FtMExporter(context, view)
    exporter.setup()
    for entity in view.entities():
        exporter.feed(entity)
    exporter.finish()
    context.close()
    store.close()

    dataset_path = settings.DATA_PATH / "datasets" / vdataset.name
    with open(dataset_path / "entities.ftm.json") as ftm:
        # it parses and finds expected number of entites
        assert len(list(read_entities(ftm))) == 11
    assert 1 == 3
    