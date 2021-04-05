from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.source import Source
from opensanctions.core.context import Context
from opensanctions.core.logs import configure_logging

# from opensanctions.core.db import Event, sync_db

__all__ = ["Dataset", "Source", "Context", "Event"]


def setup(log_level=None):
    """Configure the framework."""
    settings.DATA_PATH.mkdir(parents=True, exist_ok=True)
    configure_logging(level=log_level)
    # sync_db()
