import os
from alembic import command
from alembic.config import Config
from collections import namedtuple
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from opensanctions import settings

ENTITY_ID_LEN = 128
alembic_dir = os.path.join(os.path.dirname(__file__), "../migrate")
alembic_dir = os.path.abspath(alembic_dir)
alembic_ini = os.path.join(alembic_dir, "alembic.ini")
alembic_cfg = Config(alembic_ini)
alembic_cfg.set_main_option("script_location", alembic_dir)
alembic_cfg.set_main_option("sqlalchemy.url", settings.DATABASE_URI)

assert (
    settings.DATABASE_URI is not None
), "Need to configure $OPENSANCTIONS_DATABASE_URI."
engine = create_engine(settings.DATABASE_URI)
metadata = MetaData(bind=engine)
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base(bind=engine, metadata=metadata)

db = namedtuple("db", ["session", "metadata", "engine"])
db.session = Session()
db.metadata = metadata
db.engine = engine


def upgrade_db():
    command.upgrade(alembic_cfg, "head")


def migrate_db(message):
    command.revision(alembic_cfg, message=message, autogenerate=True)
