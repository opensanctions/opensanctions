from sqlalchemy import func, Column, Unicode, DateTime, Boolean
from sqlalchemy.dialects.postgresql import insert as upsert
from followthemoney.types import registry

from opensanctions import settings
from opensanctions.model.base import Base, db, ENTITY_ID_LEN


class Statement(Base):
    """A single statement about a property relevant to an entity.

    For example, this could be useddocker to say: "In dataset A, entity X has the
    property `name` set to 'John Smith'. I first observed this at K, and last
    saw it at L."

    Null property values are not supported. This might need to change if we
    want to support making property-less entities.
    """

    __tablename__ = "statement"

    entity_id = Column(Unicode(ENTITY_ID_LEN), index=True, primary_key=True)
    prop = Column(Unicode, primary_key=True, nullable=False)
    prop_type = Column(Unicode, nullable=False)
    schema = Column(Unicode, nullable=False)
    value = Column(Unicode, index=True, primary_key=True, nullable=False)
    dataset = Column(Unicode, primary_key=True)
    target = Column(Boolean, default=False, nullable=False)
    unique = Column(Boolean, default=False, nullable=False)
    first_seen = Column(DateTime, nullable=True)
    last_seen = Column(DateTime)

    @classmethod
    def from_entity(cls, entity, target=False, unique=False):
        values = []
        for prop, value in entity.itervalues():
            stmt = {
                "entity_id": entity.id,
                "prop": prop.name,
                "prop_type": prop.type.name,
                "schema": entity.schema.name,
                "value": value,
                "dataset": entity.dataset.name,
                "target": target,
                "unique": unique,
                "first_seen": settings.RUN_TIME,
                "last_seen": settings.RUN_TIME,
            }
            values.append(stmt)

        istmt = upsert(cls.__table__).values(values)
        stmt = istmt.on_conflict_do_update(
            index_elements=["entity_id", "prop", "value", "dataset"],
            set_=dict(
                schema=istmt.excluded.schema,
                prop_type=istmt.excluded.prop_type,
                target=istmt.excluded.target,
                unique=istmt.excluded.unique,
                last_seen=istmt.excluded.last_seen,
            ),
        )
        db.session.execute(stmt)

    @classmethod
    def all_entity_ids(cls, dataset=None, unique=None, target=None):
        q = db.session.query(cls.entity_id)
        if unique is not None:
            q = q.filter(cls.unique == unique)
        if target is not None:
            q = q.filter(cls.target == target)
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.distinct()
        return q

    @classmethod
    def all_statements(cls, dataset=None, entity_id=None):
        q = db.session.query(cls)
        if entity_id is not None:
            q = q.filter(cls.entity_id == entity_id)
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.order_by(cls.entity_id.asc())
        return q.yield_per(10000)

    @classmethod
    def all_counts(cls, dataset=None, unique=None, target=None):
        q = cls.all_entity_ids(dataset=dataset, unique=unique, target=target)
        return q.count()

    @classmethod
    def agg_target_by_country(cls, dataset=None):
        """Return the number of targets grouped by country."""
        q = db.session.query(cls.value, func.count(func.distinct(cls.entity_id)))
        # TODO: this could be generic to type values?
        q = q.filter(cls.target == True)  # noqa
        q = q.filter(cls.prop_type == registry.country.name)
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.group_by(cls.value)
        return {l: c for (l, c) in q.all()}

    @classmethod
    def agg_target_by_schema(cls, dataset=None):
        """Return the number of targets grouped by their schema."""
        # FIXME: duplicates entities when there are statements with different schema
        # defined for the same entity.
        q = db.session.query(cls.schema, func.count(func.distinct(cls.entity_id)))
        q = q.filter(cls.target == True)  # noqa
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.group_by(cls.schema)
        return {l: c for (l, c) in q.all()}

    def to_dict(self):
        return {
            "entity_id": self.entity_id,
            "prop": self.prop,
            "prop_type": self.prop_type,
            "schema": self.schema,
            "value": self.value,
            "dataset": self.value,
            "target": self.target,
            "unique": self.unique,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }
