from contextlib import contextmanager
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.types import JSON
from sqlalchemy import Table, Column, Integer, DateTime, Unicode, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as upsert_func

from opensanctions import settings

KEY_LEN = 255
VALUE_LEN = 65535
Conn = Connection

__all__ = ["Conn", "engine_tx", "create_db", "upsert_func"]

engine = create_engine(settings.DATABASE_URI, pool_size=settings.DATABASE_POOL_SIZE)


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


metadata = MetaData()

issue_table = Table(
    "issue",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("timestamp", DateTime, nullable=False),
    Column("level", Unicode(KEY_LEN), nullable=False),
    Column("module", Unicode(KEY_LEN)),
    Column("dataset", Unicode(KEY_LEN), index=True, nullable=False),
    Column("message", Unicode(VALUE_LEN)),
    Column("entity_id", Unicode(KEY_LEN), index=True),
    Column("entity_schema", Unicode(KEY_LEN)),
    Column("data", JSON, nullable=False),
)

resource_table = Table(
    "resource",
    metadata,
    Column("path", Unicode(KEY_LEN), primary_key=True, nullable=False),
    Column("dataset", Unicode(KEY_LEN), primary_key=True, index=True, nullable=False),
    Column("checksum", Unicode(KEY_LEN), nullable=False),
    Column("timestamp", DateTime, nullable=False),
    Column("mime_type", Unicode(KEY_LEN), nullable=True),
    Column("size", Integer, nullable=True),
    Column("title", Unicode(VALUE_LEN), nullable=True),
)

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

canonical_table = Table(
    "canonical",
    metadata,
    Column("entity_id", Unicode(KEY_LEN), index=True, nullable=False),
    Column("canonical_id", Unicode(KEY_LEN), index=False, nullable=True),
)


analytics_entity_table = Table(
    "analytics_entity",
    metadata,
    Column("id", Unicode(KEY_LEN), index=True, nullable=False),
    Column("schema", Unicode(KEY_LEN), nullable=False),
    Column("caption", Unicode(VALUE_LEN), nullable=False),
    Column("target", Boolean),
    Column("properties", JSONB),
    Column("first_seen", DateTime),
    Column("last_seen", DateTime),
)

analytics_dataset_table = Table(
    "analytics_dataset",
    metadata,
    Column("entity_id", Unicode(KEY_LEN), index=True, nullable=False),
    Column("dataset", Unicode(KEY_LEN), index=True, nullable=False),
)

analytics_country_table = Table(
    "analytics_country",
    metadata,
    Column("entity_id", Unicode(KEY_LEN), index=True, nullable=False),
    Column("country", Unicode(KEY_LEN), index=True, nullable=False),
)
