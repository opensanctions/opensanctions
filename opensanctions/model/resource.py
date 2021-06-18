from pantomime import parse_mimetype
from sqlalchemy import Column, Integer, Unicode, DateTime

from opensanctions import settings
from opensanctions.model.base import Base, db


class Resource(Base):
    __tablename__ = "resource"

    path = Column(Unicode, primary_key=True, nullable=False)
    checksum = Column(Unicode, primary_key=True, nullable=False)
    dataset = Column(Unicode, index=True, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    mime_type = Column(Unicode, nullable=True)
    size = Column(Integer, nullable=True)
    title = Column(Unicode, nullable=True)

    @classmethod
    def save(cls, path, dataset, checksum, mime_type, size, title):
        q = db.session.query(cls)
        q = q.filter(cls.dataset == dataset.name)
        q = q.filter(cls.path == path)
        resource = q.first()
        if resource is None:
            resource = cls()
            resource.dataset = dataset.name
            resource.path = path
        resource.mime_type = mime_type
        resource.checksum = checksum
        resource.timestamp = settings.RUN_TIME
        resource.size = size
        resource.title = title
        db.session.add(resource)

    @classmethod
    def clear(cls, dataset):
        pq = db.session.query(cls)
        pq = pq.filter(cls.dataset == dataset.name)
        pq.delete(synchronize_session=False)

    @classmethod
    def query(cls, dataset=None):
        q = db.session.query(cls)
        if dataset is not None:
            q = q.filter(cls.dataset == dataset.name)
        q = q.order_by(cls.path.asc())
        return q

    def to_dict(self):
        mime = parse_mimetype(self.mime_type)
        return {
            "path": self.path,
            "checksum": self.checksum,
            "timestamp": self.timestamp,
            "dataset": self.dataset,
            "mime_type": self.mime_type,
            "mime_label": mime.label,
            "size": self.size,
            "title": self.title,
        }
