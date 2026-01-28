from sqlalchemy.engine import Engine
from nomenklatura.db import get_engine as get_nk_engine
from nomenklatura.db import get_metadata as get_nk_metadata

from zavod.logs import get_logger

log = get_logger(__name__)
meta = get_nk_metadata()


def get_engine() -> Engine:
    """Get a SQLAlchemy engine for the given database URI."""
    return get_nk_engine()
