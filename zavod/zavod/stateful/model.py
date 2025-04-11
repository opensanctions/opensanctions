from typing import Optional, cast

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
from sqlalchemy.orm import (
    MappedAsDataclass,
    DeclarativeBase,
    Mapped,
    mapped_column,
)
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


class Base(MappedAsDataclass, DeclarativeBase):
    metadata = meta


class Program(Base):
    __tablename__ = "program"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(Unicode(KEY_LEN), nullable=False, unique=True)
    title: Mapped[Optional[str]] = mapped_column(Unicode(VALUE_LEN), nullable=True)
    url: Mapped[Optional[str]] = mapped_column(Unicode(VALUE_LEN), nullable=True)


def create_db() -> None:
    """Create all stateful database tables."""
    engine = get_engine()
    meta.create_all(
        bind=engine,
        checkfirst=True,
        tables=[
            position_table,
            statement_table,
            cast(Table, Program.__table__),
        ],
    )
