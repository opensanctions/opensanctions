import json
from typing import Any, Dict, List, Optional, Generator
from banal import ensure_list
from rigour.mime.types import JSON
from followthemoney.types import registry
import re

from zavod import Context, Entity
from zavod import helpers as h


REGEX_AUTHORITY_ID_SEP = re.compile(r"(\d+ F\.?R\.?)")


def parse_addresses(
    context: Context, addresses: List[Dict[str, str]]
) -> Generator[Entity, None, None]:
    for address in addresses:
        country_code = registry.country.clean(address.get("country"))
        city = address.get("city")
        postal_code, po_box = h.postcode_pobox(address.get("postal_code"))
        state = address.get("state")

        def contains_parts(addr):
            return (
                (city is None or city in addr)
                and (postal_code is None or postal_code in addr)
                and (po_box is None or po_box in addr)
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
                    po_box=po_box,
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
                po_box=po_box,
                region=state,
                country_code=country_code,
            )
            if addr is not None:
                yield addr


def clean_authority(value: Optional[str]) -> Optional[List[str]]:
    if value is None:
        return
    value = REGEX_AUTHORITY_ID_SEP.sub(r", \1", value)
    return h.multi_split(value, [";", ", "])


def parse_result(context: Context, result: Dict[str, Any]):
    for k, v in list(result.items()):
        if isinstance(v, str) and len(v.strip()) == 0:
            result.pop(k)

    type_ = result.pop("type", None)
    schema = context.lookup_value("type", type_)
    if schema is None:
        context.log.error("Unknown result type", type=type_)
        return
    name = result.pop("name", None)
    if name.startswith("Address 0"):
        schema = "Address"

    entity = context.make(schema)
    entity.id = context.make_slug(result.pop("id"))
    source = result.pop("source", "")
    if source.startswith("Unverified List"):
        entity.add("topics", "export.control")
    else:
        entity.add("topics", "sanction")

    entity_number = result.pop("entity_number", None)
    is_ofac = False
    if entity_number is not None:
        assert int(entity_number)
        entity.id = context.make_slug(entity_number, prefix="ofac")
        is_ofac = True

    if name is None:
        # When name is None, the rest of the row is also empty, ensure that
        assert entity_number is None and type_ is None
        return

    name = name.replace("and any successor, sub-unit, or subsidiary thereof", "")

    # Handle messed-up unicode in BIS names
    if "bis.doc.gov" in result.get("source_list_url", ""):
        name = name.replace("?s ", "'s ")
        name = name.replace("?", " ")

    name_with_information_res = context.lookup("name_with_information", name)
    if name_with_information_res is not None:
        entity.add("name", name_with_information_res.properties["name"])
        entity.add("notes", name_with_information_res.properties["notes"])
    else:
        # If it's a really long name, it's likely a name with extra info
        if len(name) > registry.name.max_length:
            context.log.warning(
                "Name long is very long, maybe it contains extra information?",
                name=name,
            )
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
        entity.add("citizenship", result.pop("citizenships", None))
        for dob in ensure_list(result.pop("dates_of_birth", "")):
            dob = h.multi_split(dob, ["circa ", " to "])
            for date in dob:
                h.apply_date(entity, "birthDate", date)
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
        h.copy_address(entity, obj)

    for ident in result.pop("ids", []):
        context.log.warning("Unknown ID type", id=ident)

    sanction = context.make("Sanction")
    sanction.id = context.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("program", result.pop("programs", []))
    sanction.add("provisions", result.pop("license_policy", []))
    sanction.add("reason", result.pop("license_requirement", []))
    sanction.add(
        "authorityId",
        clean_authority(result.pop("federal_register_notice", None)),
    )
    sanction.add("startDate", result.pop("start_date", None))
    sanction.add("endDate", result.pop("end_date", None))
    sanction.add("country", "us")
    sanction.add("authority", source)
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
