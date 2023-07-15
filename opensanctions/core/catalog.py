import yaml
from pathlib import Path
from functools import cache
from typing import Dict, Any, List
from nomenklatura.dataset import DataCatalog

from zavod.meta import get_catalog as get_zavod_catalog
from zavod.meta import Dataset

from opensanctions import settings


def _from_metadata(catalog: DataCatalog[Dataset], file_path: Path):
    with open(file_path, "r", encoding=settings.ENCODING) as fh:
        config: Dict[str, Any] = yaml.load(fh, Loader=yaml.SafeLoader)
    if "name" not in config:
        config["name"] = file_path.stem
    return Dataset(catalog, config)


@cache
def get_catalog() -> DataCatalog[Dataset]:
    """Load the current catalog of datasets and sources."""
    catalog = get_zavod_catalog()
    for glob in ("**/*.yml", "**/*.yaml"):
        for file_path in settings.METADATA_PATH.glob(glob):
            dataset = _from_metadata(catalog, file_path)
            if dataset is not None:
                catalog.add(dataset)
    return catalog


def get_dataset_names() -> List[str]:
    """Get the names for all datasets."""
    return [d.name for d in get_catalog().datasets]
