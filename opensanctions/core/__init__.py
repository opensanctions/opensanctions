from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.source import Source
from opensanctions.core.context import Context
from opensanctions.core.entity import Entity
from opensanctions.core.logs import configure_logging

from opensanctions.model.base import upgrade_db

__all__ = ["Dataset", "Source", "Context", "Entity"]


def setup(log_level=None):
    """Configure the framework."""
    settings.DATA_PATH.mkdir(parents=True, exist_ok=True)
    configure_logging(level=log_level)
    upgrade_db()
