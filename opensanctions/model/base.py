import os
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from opensanctions import settings

ENTITY_ID_LEN = 128
alembic_ini = os.path.join(os.path.dirname(__file__), "../migrate/alembic.ini")
alembic_ini = os.path.abspath(alembic_ini)
alembic_cfg = Config(alembic_ini)

assert (
    settings.DATABASE_URI is not None
), "Need to configure $OPENSANCTIONS_DATABASE_URI."
engine = create_engine(settings.DATABASE_URI)
metadata = MetaData(bind=engine)
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base(bind=engine, metadata=metadata)

db = object()
db.session = Session()


def upgrade_db():
    command.upgrade(alembic_cfg, "head")


def migrate_db(message):
    command.revision(alembic_cfg, message=message, autogenerate=True)
