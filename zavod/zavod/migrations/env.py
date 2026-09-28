"""Online-only Alembic environment for the packaged zavod migrations."""

from logging.config import fileConfig
from os import environ

from alembic import context
from alembic.runtime.environment import NameFilterParentNames, NameFilterType
from sqlalchemy import MetaData, Table, create_engine, pool
from sqlalchemy.engine import Connection

from zavod.stateful import model

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Importing the model registers the shared statement table (owned by
# nomenklatura) on ``zavod.db.meta`` as well; copy only the tables zavod
# owns onto a separate metadata so autogenerate never emits them.
STATEFUL_TABLES: tuple[Table, ...] = (
    model.position_table,
    model.program_table,
    model.review_table,
    model.review_entity_table,
)

target_metadata = MetaData()
for table in STATEFUL_TABLES:
    table.tometadata(target_metadata)

# The target database is shared with tables owned by other projects, which
# autogenerate must never see or modify; only the stateful tables that zavod
# itself declares are in scope.
ZAVOD_TABLES = frozenset(table.name for table in STATEFUL_TABLES)


def include_zavod_name(
    name: str | None,
    type_: NameFilterType,
    parent_names: NameFilterParentNames,
) -> bool:
    """Limit schema comparison to the stateful tables owned by zavod."""
    if type_ == "table":
        return name in ZAVOD_TABLES
    return True


def run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        version_table="zavod_alembic_version",
        include_name=include_zavod_name,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    database_uri = config.get_main_option("sqlalchemy.url") or environ.get(
        "ZAVOD_DATABASE_URI", environ.get("OPENSANCTIONS_DATABASE_URI")
    )
    if database_uri is None:
        raise RuntimeError(
            "No database URL configured: set sqlalchemy.url in the Alembic "
            "configuration, or ZAVOD_DATABASE_URI or OPENSANCTIONS_DATABASE_URI "
            "in the environment."
        )
    engine = create_engine(database_uri, poolclass=pool.NullPool)
    try:
        with engine.connect() as connection:
            run_migrations(connection)
    finally:
        engine.dispose()


if context.is_offline_mode():
    raise ValueError("Offline mode is not supported. Use online mode only.")

run_migrations_online()
