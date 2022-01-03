from pantomime import parse_mimetype
from sqlalchemy import delete
from sqlalchemy import Column, Integer, Unicode, DateTime

from opensanctions import settings
from opensanctions.model.base import Base, KEY_LEN, VALUE_LEN, db


class Resource(Base):
    __tablename__ = "resourcexxx"

    path = Column(Unicode(KEY_LEN), primary_key=True, nullable=False)
    dataset = Column(Unicode(KEY_LEN), primary_key=True, index=True, nullable=False)
    checksum = Column(Unicode(KEY_LEN), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    mime_type = Column(Unicode(KEY_LEN), nullable=True)
    size = Column(Integer, nullable=True)
    title = Column(Unicode(VALUE_LEN), nullable=True)

    @classmethod
    def save(cls, path, dataset, checksum, mime_type, size, title):
        q = db.session.query(cls)
        q = q.filter(cls.dataset == dataset.name)
        q = q.filter(cls.path == path)
        resource = q.first()
        if size == 0:
            if resource is not None:
                db.session.delete(resource)
            return

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
        return resource

    @classmethod
    def clear(cls, dataset):
        pq = delete(cls)
        pq = pq.where(cls.dataset == dataset.name)
        db.session.execute(pq)

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
            "sha1": self.checksum,
            "timestamp": self.timestamp,
            "dataset": self.dataset,
            "mime_type": self.mime_type,
            "mime_type_label": mime.label,
            "size": self.size,
            "title": self.title,
        }
