import os
from alembic import command
from alembic.config import Config
from contextlib import asynccontextmanager
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncConnection
from sqlalchemy.types import JSON
from sqlalchemy import Table, Column, Integer, DateTime, Unicode, Boolean
from sqlalchemy.dialects.sqlite import insert as insert_sqlite
from sqlalchemy.dialects.postgresql import insert as insert_postgresql

from opensanctions import settings
from opensanctions.util import named_semaphore

KEY_LEN = 255
VALUE_LEN = 65535
Conn = AsyncConnection
alembic_dir = os.path.join(os.path.dirname(__file__), "../migrate")
alembic_dir = os.path.abspath(alembic_dir)
alembic_ini = os.path.join(alembic_dir, "alembic.ini")
alembic_cfg = Config(alembic_ini)
alembic_cfg.set_main_option("script_location", alembic_dir)
alembic_cfg.set_main_option("sqlalchemy.url", settings.DATABASE_URI)

assert (
    settings.DATABASE_URI is not None
), "Need to configure $OPENSANCTIONS_DATABASE_URI."
engine = create_async_engine(
    settings.ASYNC_DATABASE_URI, pool_size=settings.DATABASE_POOL_SIZE
)

DIALECTS = ["sqlite", "postgresql"]
if engine.dialect.name == "sqlite":
    upsert_func = insert_sqlite
elif engine.dialect.name == "postgresql":
    upsert_func = insert_postgresql
else:
    assert engine.dialect.name in DIALECTS, "Unsupported database engine"


@asynccontextmanager
async def with_conn():
    async with named_semaphore("db", settings.DATABASE_POOL_SIZE):
        async with engine.begin() as conn:
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
    Column("dataset", Unicode(KEY_LEN), index=True),
    Column("target", Boolean, default=False, nullable=False),
    Column("unique", Boolean, default=False, nullable=False),
    Column("first_seen", DateTime, nullable=False),
    Column("last_seen", DateTime, index=True),
)


def upgrade_db():
    command.upgrade(alembic_cfg, "head")


def migrate_db(message):
    command.revision(alembic_cfg, message=message, autogenerate=True)
