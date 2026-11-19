from datetime import datetime

from zavod import settings
from zavod.context import Context
from zavod.stateful.observations import observe


def observe_item(context: Context, key: str) -> datetime | None:
    """Return when a record was first seen in this dataset's source.

    Use this to date a designation's listing where the source doesn't. A record
    removed from the source keeps its first sighting. Returns None for a record
    that was already in the source at the dataset's first observation.

    Args:
        context: The runner context.
        key: The authority-issued ID of the record, never the FtM entity ID.
    """
    assert len(key) > 0, "Observation key must not be empty"
    return observe(context.db, context.dataset.name, key, "", settings.RUN_TIME)


def observe_change(context: Context, key: str, checksum: str) -> datetime | None:
    """Return when a record was first seen with its current source content.

    Use this to date a designation's last amendment where the source doesn't. A
    new checksum returns the time of the current run. Derive the checksum from
    the same portion of the source on every run (e.g. with
    [`h.xml_node_hash`][zavod.helpers.xml_node_hash]): changing what it covers
    makes every record look amended. Returns None for content that was already
    in the source at the dataset's first observation.

    Args:
        context: The runner context.
        key: The authority-issued ID of the record, never the FtM entity ID.
        checksum: A checksum of the record's source content.
    """
    assert len(key) > 0, "Observation key must not be empty"
    assert len(checksum) > 0, "Observation checksum must not be empty"
    return observe(context.db, context.dataset.name, key, checksum, settings.RUN_TIME)
