"""Online-only Alembic environment for the zavod migrations."""

from logging.config import fileConfig
from os import environ

from alembic import context
from alembic.runtime.environment import NameFilterParentNames, NameFilterType
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import Connection

from zavod.stateful import model
from zavod.shed.funes import model as funes_model  # noqa: F401
from pravda.db import Base as pravda_base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Pravda defines its snapshot table on its own metadata; this migration
# chain owns the table even though the definition lives in the pravda
# package.
target_metadata = [model.meta, pravda_base.metadata]

# The database hosts other projects' tables, e.g. the nomenklatura resolver,
# which autogenerate must never see. The stateful and funes models register
# their tables on the shared ``zavod.db.meta``; pravda defines its snapshot
# table on its own metadata, but the table is owned by this chain.
ZAVOD_TABLES = frozenset(model.meta.tables) | frozenset(pravda_base.metadata.tables)


def include_zavod_name(
    name: str | None,
    type_: NameFilterType,
    parent_names: NameFilterParentNames,
) -> bool:
    """Limit schema comparison to the tables owned by zavod."""
    if type_ == "table":
        return name in ZAVOD_TABLES
    return True


def run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
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
