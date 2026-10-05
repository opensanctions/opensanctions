from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from nomenklatura.db import get_engine as get_nk_engine

from zavod.logs import get_logger

log = get_logger(__name__)

# Owned by zavod, not nomenklatura's shared ``get_metadata()`` singleton.
meta = MetaData()


def get_engine() -> Engine:
    """Get a SQLAlchemy engine for the given database URI."""
    return get_nk_engine()
