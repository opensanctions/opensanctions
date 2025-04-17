# TODO: Remove ignore type when https://github.com/alephdata/followthemoney/pull/1725 is in.
# Currently sqlalchemy2-stubs causes errors, and we can't drop it because FtM still supports
# on SQLAlchemy 1.x. and happens not to use any features where sqlalchemy2-stubs is broken.
# mypy: ignore-errors

from sqlalchemy import (
    Table,
    Column,
    Integer,
    DateTime,
    Unicode,
    Boolean,
    JSON,
)
from nomenklatura.statement.db import make_statement_table
from zavod.db import meta, get_engine

KEY_LEN = 255
VALUE_LEN = 65535

position_table = Table(
    "position",
    meta,
    Column("id", Integer, primary_key=True),
    Column("entity_id", Unicode(KEY_LEN), nullable=False, index=True),
    Column("caption", Unicode(VALUE_LEN), nullable=False),
    # SQLite doesn't support arrays so we use JSON
    Column("countries", JSON, nullable=False),
    Column("is_pep", Boolean, nullable=True),
    Column("topics", JSON, nullable=False),
    Column("dataset", Unicode(VALUE_LEN), nullable=False),
    Column("created_at", DateTime, nullable=False, index=True),  # Index for sorting
    # Should not be null when edited by a user, only for instances created by a crawler.
    Column("modified_at", DateTime, nullable=True),
    Column("modified_by", Unicode(KEY_LEN), nullable=True),
    Column("deleted_at", DateTime, nullable=True, index=True),  # Index for filtering
)
statement_table = make_statement_table(meta)


program_table = Table(
    "program",
    meta,
    Column("id", Integer, primary_key=True),
    Column("key", Unicode(KEY_LEN), nullable=False, unique=True),
    Column("title", Unicode(VALUE_LEN), nullable=True),
    Column("url", Unicode(VALUE_LEN), nullable=True),
)


def create_db() -> None:
    """Create all stateful database tables."""
    engine = get_engine()
    meta.create_all(
        bind=engine,
        checkfirst=True,
        tables=[
            position_table,
            statement_table,
            program_table,
        ],
    )
