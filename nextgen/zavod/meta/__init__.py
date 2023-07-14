from pathlib import Path
from functools import cache

from zavod.meta.dataset import Dataset
from zavod.meta.catalog import ArchiveBackedCatalog

__all__ = ["Dataset"]


@cache
def get_catalog() -> ArchiveBackedCatalog:
    """Get the catalog of datasets."""
    return ArchiveBackedCatalog()


def load_dataset_from_path(path: Path) -> Dataset:
    """Load a dataset from a given path."""
    return get_catalog().load_yaml(path)
