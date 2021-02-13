from opensanctions.core.target import Target
from opensanctions.core.dataset import Dataset
from opensanctions.core.context import Context
from opensanctions.core.db import Event, sync_db
from opensanctions.core.logs import configure_logging

__all__ = ["Target", "Dataset", "Context", "Event"]


def setup(log_level=None):
    """Configure the OS framework services."""
    configure_logging(level=log_level)
    sync_db()