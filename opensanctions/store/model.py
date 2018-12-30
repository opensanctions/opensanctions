import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import Column, MetaData, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import sessionmaker
from followthemoney import model

from opensanctions import settings

now = datetime.utcnow
engine = create_engine(settings.DATABASE_URI)
metadata = MetaData(bind=engine)
Session = sessionmaker(bind=engine)
Base = declarative_base(bind=engine, metadata=metadata)


class Entity(Base):
    __tablename__ = 'entity'

    id = Column(String(255), primary_key=True)
    schema = Column(String(255))
    origin = Column(String(255))
    properties = Column(String)
    context = Column(String)
    created_at = Column(DateTime, default=now)
    updated_at = Column(DateTime, default=now, onupdate=now)

    def to_dict(self):
        data = json.loads(self.context)
        data['id'] = self.id
        data['schema'] = self.schema
        data['properties'] = json.loads(self.properties)
        return data

    @hybrid_property
    def proxy(self):
        if not hasattr(self, '_proxy'):
            self._proxy = model.get_proxy(self.to_dict())
        return self._proxy

    @proxy.setter
    def proxy(self, proxy):
        self._proxy = proxy
        self.id = proxy.id
        self.schema = proxy.schema.name
        self.properties = json.dumps(proxy.properties)
        self.context = json.dumps(proxy.context)

    @classmethod
    def save(cls, session, origin, proxy):
        obj = cls.by_id(session, proxy.id)
        if obj is None:
            obj = cls()
        obj.proxy = proxy
        session.add(obj)
        return obj

    @classmethod
    def by_id(cls, session, entity_id):
        if entity_id is None:
            return
        q = session.query(cls)
        q = q.filter(cls.id == entity_id)
        return q.first()


# class Judgement(Base):
#     pass


if __name__ == '__main__':
    metadata.create_all(engine)
