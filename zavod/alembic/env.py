"""Online-only Alembic environment for the zavod migrations."""

from logging.config import fileConfig

from alembic import context
from alembic.runtime.environment import NameFilterParentNames, NameFilterType
from nomenklatura.db import get_engine
from sqlalchemy.engine import Connection

from zavod import settings
from zavod.stateful import model

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = model.meta

# The database hosts other projects' tables, e.g. the nomenklatura resolver,
# which autogenerate must never see.
ZAVOD_TABLES = frozenset(target_metadata.tables)


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
        include_name=include_zavod_name,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = config.get_main_option("sqlalchemy.url") or settings.nk.DB_URL
    engine = get_engine(url)
    with engine.connect() as connection:
        run_migrations(connection)


if context.is_offline_mode():
    raise ValueError("Offline mode is not supported. Use online mode only.")

run_migrations_online()
