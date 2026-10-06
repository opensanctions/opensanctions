"""Shared fixtures for the funes state-store tests."""

import pytest
from nomenklatura.db import Session, get_engine
from pravda.db import Base as pravda_base

from zavod.shed.funes.model import funes_meta


@pytest.fixture(scope="function")
def funes_tables() -> None:
    """For tests that reach the database through a Context, whose session
    opens its own connection: on in-memory SQLite every pooled connection
    is a separate database, so the tables must exist engine-wide. Tests
    that own a ``session`` directly use ``funes_db`` instead."""
    engine = get_engine()
    pravda_base.metadata.create_all(bind=engine)
    funes_meta.create_all(bind=engine)


@pytest.fixture(scope="function")
def funes_db(session: Session) -> Session:
    """The database session with the funes tables created. The pravda-owned
    ``snapshot`` table is created first: ``funes_attempt`` references it
    across metadata boundaries."""
    pravda_base.metadata.create_all(bind=session.connection)
    funes_meta.create_all(bind=session.connection)
    return session
