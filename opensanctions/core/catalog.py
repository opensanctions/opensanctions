import yaml
from pathlib import Path
from functools import cache
from typing import Dict, Any, List
from nomenklatura.dataset import DataCatalog

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.source import Source
from opensanctions.core.external import External
from opensanctions.core.collection import Collection


def _from_metadata(catalog: DataCatalog[Dataset], file_path: Path):
    with open(file_path, "r", encoding=settings.ENCODING) as fh:
        config: Dict[str, Any] = yaml.load(fh, Loader=yaml.SafeLoader)
    if "name" not in config:
        config["name"] = file_path.stem
    type_: str = config.get("type", Source.TYPE)
    type_ = type_.lower().strip()
    if type_ == Collection.TYPE:
        return Collection(catalog, config)
    if type_ == Source.TYPE:
        return Source(catalog, config)
    if type_ == External.TYPE:
        return External(catalog, config)


@cache
def get_catalog() -> DataCatalog[Dataset]:
    """Load the current catalog of datasets and sources."""
    catalog = DataCatalog(Dataset, {})
    for glob in ("**/*.yml", "**/*.yaml"):
        for file_path in settings.METADATA_PATH.glob(glob):
            dataset = _from_metadata(catalog, file_path)
            if dataset is not None:
                catalog.add(dataset)
    return catalog


def get_dataset_names() -> List[str]:
    """Get the names for all datasets."""
    return [d.name for d in get_catalog().datasets]
