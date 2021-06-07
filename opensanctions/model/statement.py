from sqlalchemy import Column, Unicode, DateTime
from sqlalchemy.schema import UniqueConstraint
from opensanctions.model.base import Base, ENTITY_ID_LEN


class Statement(Base):
    """A single statement about a property relevant to an entity.

    For example, this could be used to say: "In dataset A, entity X has the
    property `name` set to 'John Smith'. I first observed this at K, and last
    saw it at L."

    Null property values are not supported. This might need to change if we
    want to support making property-less entities.
    """

    __tablename__ = "statement"

    entity_id = Column(Unicode(ENTITY_ID_LEN), index=True, primary_key=True)
    canonical_id = Column(Unicode(ENTITY_ID_LEN), nullable=True, index=True)
    prop = Column(Unicode, primary_key=True)
    schema = Column(Unicode)
    value = Column(Unicode, index=True, primary_key=True)
    dataset = Column(Unicode, primary_key=True)
    first_seen = Column(DateTime, nullable=True)
    last_seen = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("entity_id", "prop", "value", "dataset", name="_prov"),
    )

    @classmethod
    def from_entity(cls, entity, dataset):
        pass
