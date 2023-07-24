from contextlib import contextmanager
from sqlalchemy.engine import Connection
from sqlalchemy import Table, Column, DateTime, Unicode, Boolean
from sqlalchemy.dialects.postgresql import insert as upsert_func
from nomenklatura.db import get_engine, get_metadata

KEY_LEN = 255
VALUE_LEN = 65535
Conn = Connection

__all__ = ["Conn", "engine_tx", "create_db", "upsert_func"]

engine = get_engine()
metadata = get_metadata()


class ConnCache(object):
    def __init__(self, conn: Conn):
        self.conn = conn

    def __hash__(self) -> int:
        return -1

    def __repr__(self) -> str:
        return "<ConnCache()>"

    def __eq__(self, other) -> bool:
        return True


def create_db():
    metadata.create_all(bind=engine)


@contextmanager
def engine_tx():
    with engine.begin() as conn:
        yield conn


@contextmanager
def engine_read():
    with engine.connect() as conn:
        yield conn


stmt_table = Table(
    "statement",
    metadata,
    Column("id", Unicode(KEY_LEN), primary_key=True, unique=True),
    Column("entity_id", Unicode(KEY_LEN), index=True, nullable=False),
    Column("canonical_id", Unicode(KEY_LEN), index=True, nullable=True),
    Column("prop", Unicode(KEY_LEN), nullable=False),
    Column("prop_type", Unicode(KEY_LEN), nullable=False),
    Column("schema", Unicode(KEY_LEN), nullable=False),
    Column("value", Unicode(VALUE_LEN), nullable=False),
    Column("original_value", Unicode(VALUE_LEN), nullable=True),
    Column("dataset", Unicode(KEY_LEN), index=True),
    Column("lang", Unicode(KEY_LEN), nullable=True),
    Column("target", Boolean, default=False, nullable=False),
    Column("external", Boolean, default=False, nullable=False),
    Column("first_seen", DateTime, nullable=False),
    Column("last_seen", DateTime, index=True),
)
