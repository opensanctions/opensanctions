from opensanctions import settings
from opensanctions.core.target import Target
from opensanctions.core.source import Source
from opensanctions.core.context import Context
from opensanctions.core.db import Event, sync_db
from opensanctions.core.logs import configure_logging

__all__ = ["Target", "Source", "Context", "Event"]


def setup(log_level=None):
    """Configure the framework."""
    settings.DATA_PATH.mkdir(parents=True, exist_ok=True)
    configure_logging(level=log_level)
    sync_db()
