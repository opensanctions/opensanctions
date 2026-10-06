"""Shared fixtures for the funes state-store tests."""

import pytest
from nomenklatura.db import Session
from pravda.db import Base as pravda_base

from zavod.shed.funes.model import funes_meta


@pytest.fixture(scope="function")
def funes_db(session: Session) -> Session:
    """The database session with the funes tables created. The pravda-owned
    ``snapshot`` table is created first: ``funes_attempt`` references it
    across metadata boundaries. Tables created here are visible to other
    sessions (e.g. ``context.db``): in-memory SQLite shares one connection
    per thread (SingletonThreadPool)."""
    pravda_base.metadata.create_all(bind=session.connection)
    funes_meta.create_all(bind=session.connection)
    return session
