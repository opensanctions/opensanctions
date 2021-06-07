from datetime import datetime

from sqlalchemy import Column, Integer, Unicode, DateTime
from opensanctions.model.base import Base, ENTITY_ID_LEN


class Event(Base):
    __tablename__ = "event"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    run_time = Column(DateTime)
    level = Column(Unicode)
    module = Column(Unicode)
    dataset = Column(Unicode)
    message = Column(Unicode)
    entity_id = Column(Unicode)
    entity_schema = Column(Unicode)
    field = Column(Unicode)
    value = Column(Unicode)
    context = Column(Unicode)
