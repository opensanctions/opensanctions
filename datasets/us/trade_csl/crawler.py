import json
from typing import Any, Dict, List, Optional, Generator
from banal import ensure_list
from pantomime.types import JSON
from followthemoney.types import registry

from zavod import Context, Entity
from zavod import helpers as h


FORMATS = ["%d %b %Y", "%d %B %Y", "%Y", "%b %Y", "%B %Y"]


def parse_date(text: Optional[str]) -> List[str]:
    if text is None or not len(text):
        return []
    text = text.replace("circa", "")
    text = text.strip()
    dates: List[str] = []
    for part in h.multi_split(text, [" to "]):
        dates.extend(h.parse_date(part, FORMATS))
    return dates


def parse_addresses(
    context: Context, addresses: List[Dict[str, str]]
) -> Generator[Entity, None, None]:
    for address in addresses:
        country_code = registry.country.clean(address.get("country"))
        city = address.get("city")
        postal_code = address.get("postal_code")
        state = address.get("state")

        def contains_parts(addr):
            return (
                (city is None or city in addr)
                and (postal_code is None or postal_code in addr)
                and (state is None or state in addr)
            )

        address_str = address.get("address")
        splits = h.multi_split(address_str, ["; and", "; "])

        if len(splits) > 0 and all([contains_parts(addr) for addr in splits]):
            for split_addr in splits:
                addr = h.make_address(
                    context,
                    full=split_addr,
                    city=city,
                    postal_code=postal_code,
                    region=state,
                    country_code=country_code,
                )
                if addr is not None:
                    yield addr
        else:
            addr = h.make_address(
                context,
                street=address_str,
                city=city,
                postal_code=postal_code,
                region=state,
                country_code=country_code,
            )
            if addr is not None:
                yield addr


def parse_result(context: Context, result: Dict[str, Any]):
    for k, v in list(result.items()):
        if isinstance(v, str) and len(v.strip()) == 0:
            result.pop(k)

    type_ = result.pop("type", None)
    schema = context.lookup_value("type", type_)
    if schema is None:
        context.log.error("Unknown result type", type=type_)
        return
    entity = context.make(schema)
    entity.id = context.make_slug(result.pop("id"))
    entity.add("topics", "sanction")

    entity_number = result.pop("entity_number", None)
    is_ofac = False
    if entity_number is not None:
        assert int(entity_number)
        entity.id = context.make_slug(entity_number, prefix="ofac")
        is_ofac = True

    name = result.pop("name", None)
    if name is not None:
        name = name.replace("and any successor, sub-unit, or subsidiary thereof", "")
        if "bis.doc.gov" in result.get("source_list_url", ""):
            name = name.replace("?s ", "'s ")
            name = name.replace("?", " ")
        entity.add("name", name)

    if is_ofac:
        context.emit(entity, target=True)
        # Don't double-import OFAC entities
        return

    for alias in ensure_list(result.pop("alt_names", "")):
        entity.add("alias", alias.split("; "))
    entity.add("notes", result.pop("remarks", None))
    entity.add("country", result.pop("country", None))
    if entity.schema.is_a("Person"):
        entity.add("position", result.pop("title", None))
        entity.add("nationality", result.pop("nationalities", None))
        entity.add("nationality", result.pop("citizenships", None))
        for dob in ensure_list(result.pop("dates_of_birth", "")):
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
    # print(result)
    assert not result.pop("nationalities", None)
    assert not result.pop("citizenships", None)
    assert not result.pop("dates_of_birth", None)
    assert not result.pop("places_of_birth", None)
    result.pop("call_sign", None)
    assert not result.pop("gross_tonnage", None)
    assert not result.pop("gross_registered_tonnage", None)
    assert not result.pop("vessel_flag", None)
    assert not result.pop("vessel_owner", None)
    assert not result.pop("vessel_type", None)

    for obj in parse_addresses(context, result.pop("addresses", [])):
        # h.apply_address(context, entity, obj)
        entity.add("address", obj.get("full"))
        for country in obj.get("country"):
            if country not in entity.countries:
                entity.add("country", country)

    for ident in result.pop("ids", []):
        context.log.warning("Unknown ID type", id=ident)

    sanction = context.make("Sanction")
    sanction.id = context.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("program", result.pop("programs", []))
    sanction.add("provisions", result.pop("license_policy", []))
    sanction.add("reason", result.pop("license_requirement", []))
    sanction.add("authorityId", result.pop("federal_register_notice", None))
    sanction.add("startDate", result.pop("start_date", None))
    sanction.add("endDate", result.pop("end_date", None))
    sanction.add("country", "us")
    sanction.add("authority", result.pop("source", None))
    sanction.add("sourceUrl", result.pop("source_information_url"))
    result.pop("source_list_url")

    context.emit(sanction)
    context.emit(entity, target=True)
    context.audit_data(result, ignore=["standard_order"])


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
        for result in data.get("results"):
            parse_result(context, result)
