from ftmstore import get_store
from sqlalchemy import Column, Integer, Unicode, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from opensanctions import settings

store = get_store(settings.DATABASE_URI)
session = sessionmaker(bind=store.engine)
Base = declarative_base(bind=store.engine, metadata=store.meta)


class Event(Base):
    __tablename__ = "event"

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    level = Column(Unicode)
    run_id = Column(Unicode)
    module = Column(Unicode)
    dataset = Column(Unicode)
    message = Column(Unicode)
    entity_id = Column(Unicode)
    entity_schema = Column(Unicode)
    field = Column(Unicode)
    value = Column(Unicode)
    context = Column(Unicode)


def sync_db():
    Base.metadata.create_all()