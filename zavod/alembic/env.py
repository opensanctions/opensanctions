"""Online-only Alembic environment for the zavod migrations."""

from logging.config import fileConfig

from alembic import context
from alembic.runtime.environment import NameFilterParentNames, NameFilterType
from nomenklatura.db import get_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection

from zavod import settings
from zavod.stateful import model
from zavod.shed.funes import model as funes_model
from pravda.db import Base as pravda_base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# The pravda snapshot table is owned by this chain even though its
# definition lives in the pravda package.
target_metadata = [model.meta, funes_model.funes_meta, pravda_base.metadata]

# The database hosts other projects' tables, e.g. the nomenklatura resolver,
# which autogenerate must never see. The sets below are the tables owned
# by this migration chain.
ZAVOD_TABLES = (
    frozenset(model.meta.tables)
    | frozenset(funes_model.funes_meta.tables)
    | frozenset(pravda_base.metadata.tables)
)


def include_zavod_name(
    name: str | None,
    type_: NameFilterType,
    parent_names: NameFilterParentNames,
) -> bool:
    """Limit schema comparison to the tables owned by zavod."""
    if type_ == "table":
        return name in ZAVOD_TABLES
    return True


# Every crawler run upgrades the database before it starts.It's probably less
# noisy if two runs don't attempt the same migration concurrently.
# Any constant works as the key, as long as nothing else uses it.
MIGRATION_LOCK_KEY = 0x7A61766F64  # "zavod"


def run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_name=include_zavod_name,
        compare_type=True,
    )
    with context.begin_transaction():
        # Held until the transaction ends. A run that waited for it then reads
        # the upgraded version table and has nothing left to do. SQLite is only
        # used in development, without concurrent connections, so it goes unlocked.
        if connection.dialect.name == "postgresql":
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:key)"),
                {"key": MIGRATION_LOCK_KEY},
            )
        context.run_migrations()


def run_migrations_online() -> None:
    url = config.get_main_option("sqlalchemy.url") or settings.nk.DB_URL
    engine = get_engine(url)
    with engine.connect() as connection:
        run_migrations(connection)


if context.is_offline_mode():
    raise ValueError("Offline mode is not supported. Use online mode only.")

run_migrations_online()
