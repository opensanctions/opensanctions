from opensanctions.core.dataset import Dataset
from opensanctions.core.context import Context
from opensanctions.core.db import Event, sync_db
from opensanctions.core.logs import configure_logging

__all__ = ["Dataset", "Context", "Event"]


def setup(quiet=False):
    """Configure the OS framework services."""
    configure_logging(quiet=quiet)
    sync_db()