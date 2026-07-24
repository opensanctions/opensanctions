from pathlib import Path
from banal import hash_data
from functools import cache
from followthemoney.exc import MetadataException

from zavod.logs import get_logger
from zavod.meta.dataset import Dataset
from zavod.meta.catalog import ArchiveBackedCatalog

__all__ = ["Dataset"]
log = get_logger(__name__)


@cache
def get_catalog() -> ArchiveBackedCatalog:
    """Get the catalog of datasets."""
    return ArchiveBackedCatalog()


def load_dataset_from_path(path: Path) -> Dataset | None:
    """Load a dataset from a given path."""
    return get_catalog().load_yaml(path)


def get_multi_dataset(names: list[str]) -> Dataset:
    """The scopes of a dataset is the set of other datasets on which analysis or
    enrichment should be performed by the runner."""
    catalog = get_catalog()
    inputs: list[Dataset] = []
    for input_name in names:
        try:
            inputs.append(catalog.require(input_name))
        except MetadataException as exc:
            log.error(
                f"Invalid dataset input: {exc}",
                input=input_name,
            )
    if not len(inputs):
        raise MetadataException(f"No valid input datasets: {names!r}")
    if len(inputs) == 1:
        return inputs[0]
    # Weird: if there are many scopes, we're making up a synthetic collection
    # to group them together so that we can build a store and view for them.
    names = sorted([i.name for i in inputs])
    key = hash_data(".".join(names))
    name = f"scope_{key[:10]}"
    if not catalog.has(name):
        data = {
            "name": name,
            "title": name,
            "datasets": names,
            "summary": "Synthetic, ad-hoc virtual collection for multiple input datasets",
            "hidden": True,
        }
        scope = catalog.make_dataset(data)
        catalog.add(scope)
    return catalog.require(name)
