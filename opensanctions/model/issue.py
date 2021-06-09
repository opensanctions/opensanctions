from banal import is_mapping
from sqlalchemy import Column, Integer, Unicode, DateTime
from sqlalchemy.dialects.postgresql import JSONB

from opensanctions import settings
from opensanctions.model.base import Base, db, ENTITY_ID_LEN


class Issue(Base):
    __tablename__ = "issue"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    level = Column(Unicode, nullable=False)
    module = Column(Unicode)
    dataset = Column(Unicode, index=True, nullable=False)
    message = Column(Unicode)
    entity_id = Column(Unicode(ENTITY_ID_LEN), index=True)
    entity_schema = Column(Unicode)
    data = Column(JSONB, nullable=False)

    @classmethod
    def save(cls, event):
        data = dict(event)
        issue = cls()
        data.pop("timestamp", None)
        issue.timestamp = settings.RUN_TIME
        issue.module = data.pop("logger", None)
        issue.level = data.pop("level")
        issue.message = data.pop("event", None)
        issue.dataset = data.pop("dataset")
        entity = data.pop("entity", None)
        if is_mapping(entity):
            issue.entity_id = entity.get("id")
            issue.entity_schema = entity.get("schema")
        elif isinstance(entity, str):
            issue.entity_id = entity
        issue.data = data
        db.session.add(issue)

    @classmethod
    def clear(cls, dataset):
        pq = db.session.query(cls)
        pq = pq.filter(cls.dataset.in_(dataset.source_names))
        pq.delete(synchronize_session=False)
