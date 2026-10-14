"""Online-only Alembic environment for the zavod migrations."""

from collections.abc import Iterable
from logging.config import fileConfig

from alembic import context
from alembic.migration import MigrationContext
from alembic.operations.ops import MigrationScript
from alembic.runtime.environment import NameFilterParentNames, NameFilterType
from alembic.script import ScriptDirectory
from nomenklatura.db import get_engine
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from zavod import settings
from zavod.stateful import model
from zavod.shed.funes import model as funes_model
from pravda.db import Base as pravda_base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Each metadata is passed explicitly: the stateful tables on the shared
# ``zavod.db.meta``, funes and pravda on their own metadata. The pravda
# snapshot table is owned by this chain even though its definition lives
# in the pravda package.
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


def number_revision(
    context: MigrationContext,
    revision: str | Iterable[str | None] | Iterable[str],
    directives: list[MigrationScript],
) -> None:
    """Give new revisions the next number in the sequence, e.g. ``0005``."""
    if getattr(config.cmd_opts, "rev_id", None) is not None:
        raise ValueError("--rev-id is not supported: env.py numbers revisions.")
    script = ScriptDirectory.from_config(config)
    last = max((int(rev.revision) for rev in script.walk_revisions()), default=0)
    for directive in directives:
        directive.rev_id = f"{last + 1:04d}"


# Revisions were renamed from Alembic's random hex IDs to a numbered sequence.
# Databases stamped with an old ID are moved to the new one before migrating.
# This can go once every database has been upgraded past the rename.
LEGACY_REVISIONS = {
    "f6e4a1f539b1": "0001",
    "a3c9d2e7b410": "0002",
    "e0dbc0b5c6a6": "0003",
    "664de5de0bc5": "0004",
}


def rename_legacy_revisions(connection: Connection) -> None:
    if not inspect(connection).has_table("alembic_version"):
        return
    for old, new in LEGACY_REVISIONS.items():
        connection.execute(
            text(
                "UPDATE alembic_version SET version_num = :new WHERE version_num = :old"
            ),
            {"old": old, "new": new},
        )


def run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_name=include_zavod_name,
        compare_type=True,
        process_revision_directives=number_revision,
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
        rename_legacy_revisions(connection)
        context.run_migrations()


def run_migrations_online() -> None:
    url = config.get_main_option("sqlalchemy.url") or settings.nk.DB_URL
    engine = get_engine(url)
    with engine.connect() as connection:
        run_migrations(connection)


if context.is_offline_mode():
    raise ValueError("Offline mode is not supported. Use online mode only.")

run_migrations_online()
