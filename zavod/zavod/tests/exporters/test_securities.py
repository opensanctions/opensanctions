import csv

from zavod.tests.exporters.util import harnessed_export
from zavod.meta import Dataset
from zavod.exporters.securities import SecuritiesExporter
from zavod import settings
from zavod.archive import clear_data_path
from zavod.crawl import crawl_dataset


def test_securities(testdataset2_export: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset2_export.name
    clear_data_path(testdataset2_export.name)

    crawl_dataset(testdataset2_export)
    harnessed_export(SecuritiesExporter, testdataset2_export)

    with open(dataset_path / "securities.csv") as csv_file:
        for row in csv.DictReader(csv_file):
            assert "id" in row
            assert "caption" in row
            assert len(row["datasets"]) > 2
