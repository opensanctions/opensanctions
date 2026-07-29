from pathlib import Path
from banal import hash_data
from functools import cache
from followthemoney.dataset import DataCatalog
from followthemoney.exc import MetadataException
from pydantic import ValidationError
from yaml import YAMLError

from zavod import settings
from zavod.exc import ConfigurationException
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


def load_directory_catalog(datasets_path: Path | None = None) -> ArchiveBackedCatalog:
    """Build a catalog from every dataset YAML found under the datasets directory.

    Use this to get the full universe of datasets — including unreleased ones and
    those outside the `default` collection — for evaluating dataset queries: the
    process-wide catalog is lazy and only knows datasets that were explicitly
    requested, so `#tag` selectors would silently match against an incomplete
    catalog. Returns a fresh catalog on every call; get_catalog() is unaffected.

    Args:
        datasets_path: Directory tree to scan; defaults to settings.DATASETS_PATH
            (the ZAVOD_DATASETS_PATH environment variable).
    """
    path = datasets_path or settings.DATASETS_PATH
    if not path.is_dir():
        raise ConfigurationException(
            f"Datasets path does not exist: {path} (set ZAVOD_DATASETS_PATH)"
        )
    catalog = ArchiveBackedCatalog()
    for yml_path in sorted(path.glob("**/*.y*ml")):
        try:
            catalog.load_yaml(yml_path, traverse_children=False)
        except (YAMLError, ValidationError, MetadataException, TypeError) as exc:
            log.warning(
                "Skipping invalid dataset YAML",
                path=yml_path.as_posix(),
                error=str(exc),
            )
    return catalog


def get_multi_dataset(catalog: DataCatalog[Dataset], names: list[str]) -> Dataset:
    """The scopes of a dataset is the set of other datasets on which analysis or
    enrichment should be performed by the runner."""
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
        catalog.make_dataset(data)
    return catalog.require(name)
