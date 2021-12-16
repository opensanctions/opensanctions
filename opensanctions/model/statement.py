import structlog
from followthemoney.types import registry
from sqlalchemy import select, func, Column, Unicode, DateTime, Boolean
from sqlalchemy.dialects.sqlite import insert as insert_sqlite
from sqlalchemy.dialects.postgresql import insert as insert_postgresql


from opensanctions import settings
from opensanctions.model.base import Base, KEY_LEN, VALUE_LEN, db

log = structlog.get_logger(__name__)


class Statement(Base):
    """A single statement about a property relevant to an entity.

    For example, this could be useddocker to say: "In dataset A, entity X has the
    property `name` set to 'John Smith'. I first observed this at K, and last
    saw it at L."

    Null property values are not supported. This might need to change if we
    want to support making property-less entities.
    """

    MAX = "max"
    BASE = "id"

    __tablename__ = "statement"

    entity_id = Column(Unicode(KEY_LEN), index=True, primary_key=True)
    canonical_id = Column(Unicode(KEY_LEN), index=True, nullable=True)
    prop = Column(Unicode(KEY_LEN), primary_key=True, nullable=False)
    prop_type = Column(Unicode(KEY_LEN), nullable=False)
    schema = Column(Unicode(KEY_LEN), nullable=False)
    value = Column(Unicode(VALUE_LEN), index=True, primary_key=True, nullable=False)
    dataset = Column(Unicode(KEY_LEN), primary_key=True, index=True)
    target = Column(Boolean, default=False, nullable=False)
    unique = Column(Boolean, default=False, nullable=False)
    first_seen = Column(DateTime, nullable=True)
    last_seen = Column(DateTime, index=True)

    @classmethod
    def from_entity(cls, entity, dataset, resolver, unique=False):
        canonical_id = resolver.get_canonical(entity.id)
        values = [
            {
                "entity_id": entity.id,
                "canonical_id": canonical_id,
                "prop": cls.BASE,
                "prop_type": cls.BASE,
                "schema": entity.schema.name,
                "value": entity.id,
                "dataset": dataset.name,
                "target": entity.target,
                "unique": unique,
                "first_seen": settings.RUN_TIME,
                "last_seen": settings.RUN_TIME,
            }
        ]
        for prop, value in entity.itervalues():
            stmt = {
                "entity_id": entity.id,
                "canonical_id": canonical_id,
                "prop": prop.name,
                "prop_type": prop.type.name,
                "schema": entity.schema.name,
                "value": value,
                "dataset": dataset.name,
                "target": entity.target,
                "unique": unique,
                "first_seen": settings.RUN_TIME,
                "last_seen": settings.RUN_TIME,
            }
            values.append(stmt)
        return values

    @classmethod
    def upsert_many(cls, values):
        if not len(values):
            return

        upsert = insert_sqlite
        if db.engine.dialect.name == "postgresql":
            upsert = insert_postgresql

        istmt = upsert(cls.__table__).values(values)
        stmt = istmt.on_conflict_do_update(
            index_elements=["entity_id", "prop", "value", "dataset"],
            set_=dict(
                canonical_id=istmt.excluded.canonical_id,
                schema=istmt.excluded.schema,
                prop_type=istmt.excluded.prop_type,
                target=istmt.excluded.target,
                unique=istmt.excluded.unique,
                last_seen=istmt.excluded.last_seen,
            ),
        )
        db.session.execute(stmt)

    @classmethod
    def all_ids(cls, dataset=None, unique=None, target=None):
        q = db.session.query(cls.canonical_id)
        q = q.filter(cls.prop == cls.BASE)
        if unique is not None:
            q = q.filter(cls.unique == unique)
        if target is not None:
            q = q.filter(cls.target == target)
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.distinct()
        return q.yield_per(10000)

    @classmethod
    def all_statements(cls, dataset=None, canonical_id=None, inverted_ids=None):
        table = cls.__table__
        q = select(table)
        if canonical_id is not None:
            q = q.filter(table.c.canonical_id == canonical_id)
        if inverted_ids is not None:
            alias = table.alias()
            sq = select(func.distinct(alias.c.canonical_id))
            sq = sq.filter(alias.c.prop_type == registry.entity.name)
            sq = sq.filter(alias.c.value.in_(inverted_ids))
            # sq = sq.subquery()
            # cte = select(func.distinct(cls.canonical_id).label("canonical_id"))
            # cte = cte.where(cls.prop_type == registry.entity.name)
            # cte = cte.where(cls.value.in_(inverted_ids))
            # cte = cte.cte("inverted")
            # Find entities which refer to the given entity in one of their
            # property values.
            # inverted = aliased(cls)
            q = q.filter(table.c.canonical_id.in_(sq))
            # q = q.filter(inverted.prop_type == registry.entity.name)
            # q = q.filter(inverted.value.in_(inverted_ids))
        if dataset is not None:
            q = q.filter(table.c.dataset.in_(dataset.source_names))
        q = q.order_by(table.c.canonical_id.asc())
        res = db.session.execute(q)
        while True:
            batch = res.fetchmany(10000)
            if not batch:
                break
            yield from batch

    @classmethod
    def all_counts(cls, dataset=None, unique=None, target=None):
        q = cls.all_ids(dataset=dataset, unique=unique, target=target)
        return q.count()

    @classmethod
    def agg_target_by_country(cls, dataset=None):
        """Return the number of targets grouped by country."""
        count = func.count(func.distinct(cls.canonical_id))
        q = db.session.query(cls.value, count)
        # TODO: this could be generic to type values?
        q = q.filter(cls.target == True)  # noqa
        q = q.filter(cls.prop_type == registry.country.name)
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.group_by(cls.value)
        q = q.order_by(count.desc())
        return q.all()

    @classmethod
    def agg_target_by_schema(cls, dataset=None):
        """Return the number of targets grouped by their schema."""
        # FIXME: duplicates entities when there are statements with different schema
        # defined for the same entity.
        count = func.count(func.distinct(cls.canonical_id))
        q = db.session.query(cls.schema, count)
        q = q.filter(cls.target == True)  # noqa
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.group_by(cls.schema)
        q = q.order_by(count.desc())
        return q.all()

    @classmethod
    def all_schemata(cls, dataset=None):
        """Return all schemata present in the dataset"""
        q = db.session.query(cls.schema)
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.group_by(cls.schema)
        return [s for (s,) in q.all()]

    @classmethod
    def max_last_seen(cls, dataset=None):
        """Return the latest date of the data."""
        q = db.session.query(func.max(cls.last_seen))
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        for (value,) in q.all():
            return value

    @classmethod
    def entities_datasets(cls, dataset=None):
        """Return all entity IDs with the dataset they belong to."""
        q = db.session.query(cls.entity_id, cls.dataset)
        if dataset is not None:
            q = q.filter(cls.dataset.in_(dataset.source_names))
        q = q.distinct()
        return q

    @classmethod
    def cleanup_dataset(cls, dataset):
        db.session.flush()
        # set the entity BASE to the earliest spotting of the entity:
        # table = cls.__table__
        # cte = select(
        #     func.min(table.c.first_seen).label("first_seen"),
        #     table.c.entity_id.label("entity_id"),
        # )
        # cte = cte.where(table.c.dataset == dataset.name)
        # cte = cte.group_by(table.c.entity_id)
        # cte = cte.cte("seen")
        # sq = select(cte.c.first_seen)
        # sq = sq.where(cte.c.entity_id == table.c.entity_id)
        # sq = sq.limit(1)
        # q = update(table)
        # q = q.where(table.c.dataset == dataset.name)
        # q = q.where(table.c.prop == cls.BASE)
        # q = q.values({table.c.first_seen: sq.scalar_subquery()})
        # # log.info("Setting BASE first_seen...", q=str(q))
        # db.session.execute(q)

        # remove non-current statements (in the future we may want to keep them?)
        max_last_seen = cls.max_last_seen(dataset=dataset)
        if max_last_seen is not None:
            pq = db.session.query(cls)
            pq = pq.filter(cls.dataset == dataset.name)
            pq = pq.filter(cls.last_seen < max_last_seen)
            pq.delete(synchronize_session=False)

    @classmethod
    def resolve_all(cls, resolver):
        log.info("Resolving canonical_id in statements...", resolver=resolver)
        q = db.session.query(cls)
        q = q.filter(cls.canonical_id != cls.entity_id)
        q.update({cls.canonical_id: cls.entity_id})
        for canonical in resolver.canonicals():
            referents = resolver.get_referents(canonical)
            log.debug("Resolving: %s" % canonical.id, referents=referents)
            q = db.session.query(cls)
            q = q.filter(cls.entity_id.in_(referents))
            q = q.update({cls.canonical_id: canonical.id})
        db.session.commit()

    @classmethod
    def resolve(cls, resolver, canonical_id):
        referents = resolver.get_referents(canonical_id)
        q = db.session.query(cls)
        q = q.filter(cls.entity_id.in_(referents))
        q = q.update({cls.canonical_id: canonical_id})
        db.session.commit()

    @classmethod
    def clear(cls, dataset):
        pq = db.session.query(cls)
        # TODO: should this do collections?
        pq = pq.filter(cls.dataset == dataset.name)
        pq.delete(synchronize_session=False)

    @classmethod
    def unique_conflict(cls, left_ids, right_ids):
        cteq = select(
            func.distinct(cls.entity_id).label("entity_id"),
            func.max(cls.unique).label("unique"),
            cls.dataset.label("dataset"),
        )
        cteq = cteq.where(cls.prop == cls.BASE)
        cte = cteq.cte("uniques")
        # sqlite 3.35 -
        # cte = cte.prefix_with("MATERIALIZED")
        left = cte.alias("left")
        right = cte.alias("right")
        q = select([left.c.entity_id, right.c.entity_id])
        q = q.where(left.c.dataset == right.c.dataset)
        q = q.where(left.c.unique == True)
        q = q.where(right.c.unique == True)
        q = q.where(left.c.entity_id.in_(left_ids))
        q = q.where(right.c.entity_id.in_(right_ids))
        # print(q)
        for _ in db.engine.execute(q):
            return True
        return False

    def to_dict(self):
        return {
            "entity_id": self.entity_id,
            "canonical_id": self.canonical_id,
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
