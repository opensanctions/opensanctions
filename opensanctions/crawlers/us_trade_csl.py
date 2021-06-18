import json
from pprint import pprint  # noqa

from opensanctions.util import jointext
from opensanctions.util import date_formats, DAY


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
        parts = (
            address.get("address"),
            address.get("city"),
            address.get("postal_code"),
            address.get("state"),
        )
        entity.add("address", jointext(*parts, sep=", "))
        entity.add("country", address.get("country", None))

    for ident in result.pop("ids", []):
        country = ident.get("country")
        value = ident.get("number")
        idres = context.lookup("ids", ident.get("type"))
        if idres is None:
            context.log.warning(
                "Unknown ID type",
                entity=entity,
                type=ident.get("type"),
                value=value,
                country=country,
            )
            continue
        if idres.prop is not None:
            entity.add(idres.prop, value)
            entity.add("country", country)
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
