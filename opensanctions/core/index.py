import structlog
from pathlib import Path
from opensanctions import settings
from nomenklatura.loader import Loader
from nomenklatura.index import Index

from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset

log = structlog.get_logger(__name__)


def get_index_path(dataset: Dataset) -> Path:
    index_dir = settings.DATA_PATH.joinpath("index")
    index_dir.mkdir(exist_ok=True)
    return index_dir.joinpath(f"{dataset.name}.pkl")


def get_index(
    dataset: Dataset, loader: Loader[Dataset, Entity]
) -> Index[Dataset, Entity]:
    """Load the search index for the given dataset or generate one if it does
    not exist."""
    path = get_index_path(dataset)
    index = Index.load(loader, path)
    return index
