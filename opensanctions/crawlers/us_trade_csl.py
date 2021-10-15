import json
from functools import lru_cache
from pantomime.types import JSON
from requests.exceptions import TooManyRedirects

from opensanctions.core import Dataset
from opensanctions import helpers as h

FORMATS = ["%d %b %Y", "%d %B %Y", "%Y", "%b %Y", "%B %Y"]
SDN = Dataset.get("us_ofac_sdn")


@lru_cache(maxsize=None)
def deref_url(context, url):
    try:
        res = context.http.get(url, stream=True)
        return res.url
    except TooManyRedirects:
        return url


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
        assert int(entity_number)
        entity.id = SDN.make_slug(entity_number)

    entity.add("name", result.pop("name", None))
    entity.add("alias", result.pop("alt_names", None))
    entity.add("notes", result.pop("remarks", None))
    entity.add("country", result.pop("country", None))
    if entity.schema.is_a("Person"):
        entity.add("position", result.pop("title", None))
        entity.add("nationality", result.pop("nationalities", None))
        entity.add("nationality", result.pop("citizenships", None))
        for dob in result.pop("dates_of_birth", []):
            entity.add("birthDate", h.parse_date(dob, FORMATS))
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
        country = ident.pop("country")
        entity.add("country", country)
        h.apply_feature(
            context,
            entity,
            ident.pop("type"),
            ident.pop("number"),
            country=country,
            date_formats=FORMATS,
            start_date=ident.pop("issue_date", None),
            end_date=ident.pop("expiration_date", None),
        )

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
    source_url = deref_url(context, result.pop("source_information_url"))
    sanction.add("sourceUrl", source_url)
    result.pop("source_list_url")

    # TODO: what is this?
    result.pop("standard_order", None)

    context.emit(sanction)
    context.emit(entity, target=True)

    if len(result):
        context.pprint(result)


def crawl(context):
    path = context.fetch_resource("source.json", context.dataset.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
        for result in data.get("results"):
            parse_result(context, result)
