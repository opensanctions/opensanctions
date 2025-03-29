from functools import cache
from sqlalchemy.engine import Engine
from nomenklatura.db import get_engine as get_nk_engine
from nomenklatura.db import get_metadata as get_nk_metadata

from zavod import settings
from zavod.logs import get_logger

log = get_logger(__name__)
meta = get_nk_metadata()


@cache
def get_engine() -> Engine:
    """Get a SQLAlchemy engine for the given database URI."""
    get_nk_engine.cache_clear()
    return get_nk_engine(settings.DATABASE_URI)
