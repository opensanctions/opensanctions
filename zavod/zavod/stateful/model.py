from sqlalchemy import (
    Table,
    Column,
    Integer,
    DateTime,
    Unicode,
    Boolean,
    JSON,
    Index,
    text,
)
from nomenklatura.db import make_statement_table
from zavod.db import meta, get_engine

KEY_LEN = 255
VALUE_LEN = 65535
LARGE_VALUE_LEN = 1024 * 1024

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


review_table = Table(
    "review",
    meta,
    Column("id", Integer, primary_key=True),
    Column("key", Unicode(KEY_LEN), nullable=False, index=True),
    Column("dataset", Unicode(KEY_LEN), nullable=False, index=True),
    Column("extraction_checksum", Unicode(KEY_LEN), nullable=False),
    Column("extraction_schema", JSON, nullable=False),
    Column("source_value", Unicode(LARGE_VALUE_LEN), nullable=True),
    Column("source_mime_type", Unicode(VALUE_LEN), nullable=True),
    Column("source_label", Unicode(VALUE_LEN), nullable=True),
    Column("source_url", Unicode(VALUE_LEN), nullable=True),
    Column("accepted", Boolean, nullable=False, index=True),
    Column("crawler_version", Integer, nullable=False),
    Column("orig_extraction_data", JSON, nullable=False),
    Column("extracted_data", JSON, nullable=False),
    Column("last_seen_version", Unicode(KEY_LEN), nullable=False, index=True),
    Column("modified_at", DateTime, nullable=False),
    Column("modified_by", Unicode(KEY_LEN), nullable=False),
    Column("deleted_at", DateTime, nullable=True, index=True),
)


Index(
    "ix_review_key_dataset_unique_not_deleted",
    review_table.c.key,
    review_table.c.dataset,
    unique=True,
    sqlite_where=text("deleted_at IS NULL"),
    postgresql_where=text("deleted_at IS NULL"),
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
            review_table,
        ],
    )
