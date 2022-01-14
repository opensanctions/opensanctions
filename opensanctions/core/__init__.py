import asyncio
import structlog
from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.source import Source
from opensanctions.core.context import Context
from opensanctions.core.entity import Entity
from opensanctions.core.logs import configure_logging

from opensanctions.core.db import create_db

__all__ = ["Dataset", "Source", "Context", "Entity"]


def setup(log_level=None):
    """Configure the framework."""
    configure_logging(level=log_level)
    log = structlog.get_logger(__name__)
    log.debug(
        "OpenSanctions starting",
        database_uri=settings.DATABASE_URI,
        data_path=str(settings.DATA_PATH),
        datasets=Dataset.names(),
    )
    # asyncio.run(create_db())
