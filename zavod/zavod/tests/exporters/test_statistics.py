from json import load

from zavod import settings
from zavod.archive import clear_data_path
from zavod.exporters.statistics import StatisticsExporter
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset
from zavod.tests.exporters.util import harnessed_export


def test_statistics(testdataset1: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)

    crawl_dataset(testdataset1)
    harnessed_export(StatisticsExporter, testdataset1)

    with open(dataset_path / "statistics.json") as statistics_file:
        statistics = load(statistics_file)

    assert statistics["entity_count"] == 11
    assert statistics["target_count"] == 7
    assert "Organization" in statistics["schemata"]
    assert "Person" in statistics["schemata"]
    assert len(statistics["schemata"]) == 6

    thing_countries = statistics["things"]["countries"]
    assert {"code": "de", "count": 2, "label": "Germany"} in thing_countries
    assert {"code": "ca", "count": 1, "label": "Canada"} in thing_countries
    assert len(thing_countries) == 6

    thing_schemata = statistics["things"]["schemata"]
    assert {
        "name": "Person",
        "count": 6,
        "label": "Person",
        "plural": "People",
    } in thing_schemata
    assert len(thing_schemata) == 3

    target_countries = statistics["targets"]["countries"]
    assert {"code": "de", "count": 2, "label": "Germany"} in target_countries
    assert "ca" not in {f["code"] for f in target_countries}
    assert len(target_countries) == 5

    target_schemata = statistics["targets"]["schemata"]
    assert {
        "name": "Person",
        "count": 5,
        "label": "Person",
        "plural": "People",
    } in target_schemata
    assert len(target_schemata) == 3
