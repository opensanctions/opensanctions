import pytest
import shutil

from zavod.archive import dataset_resource_path
from zavod.meta import Dataset, DataResource
from zavod.runtime.resources import DatasetResources

from zavod.tests.conftest import DATASET_1_YML

CSV_PATH = DATASET_1_YML.parent / "dataset.csv"


def test_resources(vdataset: Dataset):
    resources = DatasetResources(vdataset)
    resources.clear()
    assert len(resources.all()) == 0

    with pytest.raises(ValueError):
        DataResource.from_path(vdataset, CSV_PATH)

    resource_path = dataset_resource_path(vdataset.name, "dataset.csv")
    shutil.copyfile(CSV_PATH, resource_path)

    resource = DataResource.from_path(vdataset, resource_path)
    assert resource.name == "dataset.csv"
    assert resource.size is not None
    assert resource.size > 0
    assert resource.checksum == "085f67f2e868f73964b66addd9f4b8584d59a10c"

    resources.save(resource)
    assert len(resources.all()) == 1
    assert resources.all()[0].name == "dataset.csv"
    resources.save(resource)
    assert len(resources.all()) == 1

    resources2 = DatasetResources(vdataset)
    assert len(resources2.all()) == 1
    assert resources2.all()[0].name == "dataset.csv"

    resources.clear()
    assert len(resources.all()) == 0
    assert len(resources2.all()) == 0
