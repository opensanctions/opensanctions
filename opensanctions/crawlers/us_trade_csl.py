import json
from pprint import pprint  # noqa
from followthemoney.types import registry

from opensanctions.helpers import make_address
from opensanctions.util import date_formats, DAY, MONTH, YEAR

FORMATS = [("%d %b %Y", DAY), ("%d %B %Y", DAY), ("%Y", YEAR), ("%b %Y", MONTH)]
FORMATS = FORMATS + [("%B %Y", MONTH)]


def parse_date(date):
    return date_formats(date, FORMATS)


def parse_result(context, result):
    type_ = result.pop("type", None)
    schema = context.lookup_value("type", type_)
    if schema is None:
        context.log.error("Unknown result type", type=type_)
        return
    entity = context.make(schema)
    entity.make_slug(result.pop("id"))

    result.pop("entity_number", None)

    entity.add("name", result.pop("name", None))
    entity.add("alias", result.pop("alt_names", None))
    entity.add("summary", result.pop("remarks", None))
    entity.add("country", result.pop("country", None))
    if entity.schema.is_a("Person"):
        entity.add("position", result.pop("title", None))
        entity.add("nationality", result.pop("nationalities", None))
        entity.add("nationality", result.pop("citizenships", None))
        entity.add("birthDate", result.pop("dates_of_birth", None))
        entity.add("birthPlace", result.pop("places_of_birth", None))
    elif entity.schema.is_a("Vessel"):
        entity.add("flag", result.pop("vessel_flag", None))
        entity.add("callSign", result.pop("call_sign", None))
        entity.add("type", result.pop("vessel_type", None))
        grt = result.pop("gross_registered_tonnage", None)
        entity.add("grossRegisteredTonnage", grt)
        gt = result.pop("gross_tonnage", None)
        entity.add("tonnage", gt)

        # TODO: make adjacent owner entity
        result.pop("vessel_owner", None)

    assert result.pop("title", None) is None
    assert not len(result.pop("nationalities", []))
    assert not len(result.pop("citizenships", []))
    assert not len(result.pop("dates_of_birth", []))
    assert not len(result.pop("places_of_birth", []))

    for address in result.pop("addresses", []):
        obj = make_address(
            context,
            street=address.get("address"),
            city=address.get("city"),
            postal_code=address.get("postal_code"),
            region=address.get("state"),
            country=address.get("country"),
        )
        if obj is not None:
            context.emit(obj)
            entity.add("addressEntity", obj)
            entity.add("country", obj.get("country"))

    for ident in result.pop("ids", []):
        country = ident.get("country")
        value = ident.get("number")
        type_ = ident.get("type")
        idres = context.lookup("ids", type_)
        if idres is None:
            context.log.warning(
                "Unknown ID type",
                entity=entity,
                type=type_,
                value=value,
                country=country,
            )
            continue
        if idres.nested is not None:
            adj = context.make(idres.schema)
            adj.make_id(type_, value)
            if idres.type is not None:
                adj.add(idres.type, type_)
            adj.add(idres.value, value)
            entity.add(idres.nested, adj)
            context.emit(adj)
        elif idres.backref is not None:
            adj = context.make(idres.schema)
            adj.make_id(type_, value)
            adj.add(idres.backref, entity)
            if idres.type is not None:
                adj.add(idres.type, type_)
            adj.add(idres.value, value)
            context.emit(adj)
        else:
            if idres.schema is not None:
                entity.add_schema(idres.schema)
            entity.add("country", country)
            if idres.prop is not None:
                prop = entity.schema.get(idres.prop)
                if prop.type == registry.date:
                    value = parse_date(value)
                entity.add(idres.prop, value)

        # pprint(ident)

    sanction = context.make("Sanction")
    sanction.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("program", result.pop("programs", []))
    sanction.add("status", result.pop("license_policy", []))
    sanction.add("reason", result.pop("license_requirement", []))
    sanction.add("reason", result.pop("federal_register_notice", None))
    sanction.add("startDate", result.pop("start_date", None))
    sanction.add("endDate", result.pop("end_date", None))
    sanction.add("country", "us")
    sanction.add("authority", result.pop("source"))
    # TODO: deref
    sanction.add("sourceUrl", result.pop("source_information_url"))
    result.pop("source_list_url")

    # TODO: what is this?
    result.pop("standard_order", None)

    context.emit(sanction)
    context.emit(entity, target=True)

    if len(result):
        pprint(result)
    # title = result.get("title", None)
    # if title is not None:
    #     print(title)


def crawl(context):
    context.fetch_resource("source.json", context.dataset.data.url)
    path = context.get_resource_path("source.json")
    with open(path, "r") as file:
        data = json.load(file)
        for result in data.get("results"):
            parse_result(context, result)
