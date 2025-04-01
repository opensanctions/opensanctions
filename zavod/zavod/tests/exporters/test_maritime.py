import csv

from zavod.tests.exporters.util import harnessed_export
from zavod.meta import Dataset
from zavod.exporters.maritime import MaritimeExporter
from zavod import settings
from zavod.archive import clear_data_path
from zavod.crawl import crawl_dataset


def test_maritime(testdataset_maritime: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset_maritime.name
    clear_data_path(testdataset_maritime.name)

    crawl_dataset(testdataset_maritime)
    harnessed_export(MaritimeExporter, testdataset_maritime)

    with open(dataset_path / "maritime.csv") as csv_file:
        types = {}
        for row in csv.DictReader(csv_file):
            assert "type" in row
            if row["type"] not in types:
                types[row["type"]] = 0
            types[row["type"]] += 1
            assert "id" in row
            assert "imo" in row
            assert "caption" in row
        assert types["VESSEL"] == 4
        assert types["ORGANIZATION"] == 1
