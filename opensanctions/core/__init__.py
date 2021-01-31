from opensanctions.core.dataset import Dataset
from opensanctions.core.context import Context
from opensanctions.core.db import Event, sync_db
from opensanctions.core.logs import configure_logging

__all__ = ["Dataset", "Context"]


def setup():
    """Configure the OS framework services."""
    configure_logging()
    sync_db()