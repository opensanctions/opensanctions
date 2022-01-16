from banal import ensure_list
from followthemoney.types import registry

from opensanctions.core.lookups import common_lookups
from opensanctions.helpers.dates import parse_date
from opensanctions.helpers.gender import clean_gender


def _prepare_value(prop, values, date_formats):
    prepared = []
    for value in ensure_list(values):
        if prop.name == "gender":
            prepared.extend(clean_gender(value))
            continue

        if prop.type == registry.date:
            prepared.extend(parse_date(value, date_formats))
            continue
        prepared.append(value)
    return prepared


def apply_feature(
    context,
    entity,
    feature,
    values,
    country=None,
    start_date=None,
    end_date=None,
    comment=None,
    authority=None,
    date_formats=[],
):
    """This is pretty specific to the needs of OFAC/CSL data."""
    feature = feature.replace("Digital Currency Address - ", "")
    lookup = common_lookups().get("features")
    result = lookup.match(feature)
    if result is None:
        context.log.warning(
            "Missing feature",
            entity=entity,
            schema=entity.schema,
            feature=feature,
            values=values,
        )
        return
    if result.schema is not None:
        # The presence of this feature implies that the entity has a
        # certain schema.
        entity.add_schema(result.schema)
    if result.prop is not None:
        # Set a property directly on the entity.
        prop = entity.schema.get(result.prop)
        prepared = _prepare_value(prop, values, date_formats)
        entity.add(prop, prepared)

    nested = result.nested
    if nested is not None:
        # So this is deeply funky: basically, nested entities are
        # mapped from
        adj = context.make(nested.get("schema"))
        adj.id = context.make_id(entity.id, feature, values)
        if nested.get("singleton", False):
            adj.id = context.make_id(entity.id, adj.schema.name)

        value_prop = adj.schema.get(nested.get("value"))
        assert value_prop is not None, nested
        prepared = _prepare_value(value_prop, values, date_formats)
        adj.add(value_prop, prepared)

        if nested.get("feature") is not None:
            adj.add(nested.get("feature"), feature)

        if nested.get("country") is not None:
            adj.add(nested.get("country"), country)

        if nested.get("backref") is not None:
            backref_prop = adj.schema.get(nested.get("backref"))
            assert entity.schema.is_a(backref_prop.range), (
                entity.schema,
                backref_prop.range,
                feature,
                values,
            )
            adj.add(backref_prop, entity.id)

        if nested.get("forwardref") is not None:
            entity.add(nested.get("forwardref"), adj.id)

        adj.add("startDate", start_date, quiet=True)
        adj.add("endDate", end_date, quiet=True)
        adj.add("description", comment, quiet=True)
        adj.add("authority", authority, quiet=True)

        context.emit(adj)
        return adj
