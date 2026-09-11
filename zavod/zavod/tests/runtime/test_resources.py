import pytest
import shutil

from followthemoney.dataset import Version

from zavod import settings
from zavod.archive import dataset_resource_path
from zavod.meta import Dataset
from zavod.runtime.resources import DatasetResources
from zavod.tests.util import get_manifest

from zavod.tests.conftest import DATASET_1_YML

CSV_PATH = DATASET_1_YML.parent / "dataset.csv"


def test_resources(testdataset1: Dataset):
    version = settings.RUN_VERSION
    get_manifest(testdataset1, version)
    resources = DatasetResources(testdataset1, version)
    assert len(resources.all()) == 0

    with pytest.raises(ValueError):
        testdataset1.resource_from_path(CSV_PATH)

    resource_path = dataset_resource_path(testdataset1.name, "dataset.csv")
    shutil.copyfile(CSV_PATH, resource_path)

    resource = testdataset1.resource_from_path(resource_path)
    assert resource.name == "dataset.csv"
    assert resource.size is not None
    assert resource.size > 0
    assert resource.checksum == "2600ca8d5af7ada55818127c204169b388d20707", (
        resource_path
    )

    resources.save(resource)
    assert len(resources.all()) == 1
    assert resources.all()[0].name == "dataset.csv"
    resources.save(resource)
    assert len(resources.all()) == 1

    resources2 = DatasetResources(testdataset1, version)
    assert len(resources2.all()) == 1
    assert resources2.all()[0].name == "dataset.csv"

    # A fresh run is a fresh version, and starts with no resources:
    resources3 = DatasetResources(testdataset1, Version.new("bbb"))
    assert len(resources3.all()) == 0
