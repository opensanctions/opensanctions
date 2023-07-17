from zavod import settings
from zavod.logs import configure_logging, get_logger
from zavod.entity import Entity
from zavod.meta import Dataset

from opensanctions.core.context import Context
from opensanctions.core.db import create_db
from opensanctions.core.catalog import get_catalog, get_dataset_names

__all__ = ["Dataset", "Source", "Context", "Entity", "get_catalog"]


def setup(log_level=None):
    """Configure the framework."""
    configure_logging(level=log_level)
    log = get_logger(__name__)
    log.debug(
        "OpenSanctions starting",
        data_path=str(settings.DATA_PATH),
        datasets=get_dataset_names(),
    )
    create_db()
