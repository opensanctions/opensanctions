from typing import List
from functools import cache
from nomenklatura.dataset import DataCatalog

from zavod.meta import get_catalog as get_zavod_catalog
from zavod.meta import load_dataset_from_path
from zavod.meta import Dataset

from opensanctions import settings


@cache
def get_catalog() -> DataCatalog[Dataset]:
    """Load the current catalog of datasets and sources."""
    catalog = get_zavod_catalog()
    for glob in ("**/*.yml", "**/*.yaml"):
        for file_path in settings.METADATA_PATH.glob(glob):
            load_dataset_from_path(file_path)
    return catalog


def get_dataset_names() -> List[str]:
    """Get the names for all datasets."""
    return [d.name for d in get_catalog().datasets]
