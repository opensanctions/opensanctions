"""Shared fixtures for the funes state-store tests."""

import pytest
from nomenklatura.db import get_engine
from pravda.db import Base as pravda_base

from zavod.shed.funes.model import funes_meta


@pytest.fixture(scope="function")
def funes_db() -> None:
    """The funes tables, created engine-bound like ``zavod_db``: the DDL
    commits inside ``engine.begin()``, so the schema is visible to every
    later pooled connection (e.g. ``context.db``), not only to the one
    that created it. The pravda-owned ``snapshot`` table is created
    first: ``funes_attempt`` references it across metadata boundaries."""
    pravda_base.metadata.create_all(bind=get_engine())
    funes_meta.create_all(bind=get_engine())
