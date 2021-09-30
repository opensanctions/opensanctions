import json
from pprint import pprint  # noqa
from followthemoney.types import registry
from followthemoney.dedupe import Judgement
from pantomime.types import JSON
from prefixdate import parse_formats

from opensanctions.core import Dataset
from opensanctions import helpers as h

FORMATS = ["%d %b %Y", "%d %B %Y", "%Y", "%b %Y", "%B %Y"]


def parse_date(date):
    parsed = parse_formats(date, FORMATS)
    if parsed.text is not None:
        return parsed.text
    return h.extract_years(date, date)


def decide_sdn_mappings(context, entity, sdn_id):
    assert int(sdn_id)
    dataset = Dataset.get("us_ofac_sdn")
    sdn_id = dataset.make_slug(sdn_id)
    judgement = context.resolver.get_judgement(entity.id, sdn_id)

    if judgement == Judgement.NEGATIVE:
        context.log.warning("SDN/CSL Contradiction", sdn_id=sdn_id, csl_id=entity.id)
        context.resolver.explode(entity.id)
        context.resolver.explode(sdn_id)
        return

    context.resolver.decide(
        sdn_id, entity.id, judgement=Judgement.POSITIVE, user="csl_automatch"
    )


def parse_result(context, result):
    type_ = result.pop("type", None)
    schema = context.lookup_value("type", type_)
    if schema is None:
        context.log.error("Unknown result type", type=type_)
        return
    entity = context.make(schema)
    entity.id = context.make_slug(result.pop("id"))

    entity_number = result.pop("entity_number", None)
    if entity_number is not None:
        decide_sdn_mappings(context, entity, entity_number)
        return

    entity.add("name", result.pop("name", None))
    entity.add("alias", result.pop("alt_names", None))
    entity.add("notes", result.pop("remarks", None))
    entity.add("country", result.pop("country", None))
    if entity.schema.is_a("Person"):
        entity.add("position", result.pop("title", None))
        entity.add("nationality", result.pop("nationalities", None))
        entity.add("nationality", result.pop("citizenships", None))
        for dob in result.pop("dates_of_birth", []):
            entity.add("birthDate", parse_date(dob))
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
        obj = h.make_address(
            context,
            street=address.get("address"),
            city=address.get("city"),
            postal_code=address.get("postal_code"),
            region=address.get("state"),
            country=address.get("country"),
        )
        h.apply_address(context, entity, obj)

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
            adj.id = context.make_id(type_, value)
            if idres.type is not None:
                adj.add(idres.type, type_)
            adj.add(idres.value, value)
            entity.add(idres.nested, adj)
            context.emit(adj)
        elif idres.backref is not None:
            adj = context.make(idres.schema)
            adj.id = context.make_id(type_, value)
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
    sanction.id = context.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("program", result.pop("programs", []))
    sanction.add("status", result.pop("license_policy", []))
    sanction.add("reason", result.pop("license_requirement", []))
    sanction.add("reason", result.pop("federal_register_notice", None))
    sanction.add("startDate", result.pop("start_date", None))
    sanction.add("endDate", result.pop("end_date", None))
    sanction.add("country", "us")
    sanction.add("authority", result.pop("source", None))

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
    path = context.fetch_resource("source.json", context.dataset.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
        for result in data.get("results"):
            parse_result(context, result)

    context.resolver.save()
