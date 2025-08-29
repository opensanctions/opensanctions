import json
from typing import Any, Dict, List, Optional, Generator
from banal import ensure_list
from rigour.mime.types import JSON
from followthemoney.types import registry
import re

from zavod import Context, Entity
from zavod import helpers as h

# Programs emitted by this crawler (is_ofac=False):
# These are non-OFAC sanctions or export control lists where entities are emitted
# with their own unique internal identifiers.
#
#     "Nonproliferation Sanctions (ISN) - State Department",
#     "ITAR Debarred (DTC) - State Department",
#     "Unverified List (UVL) - Bureau of Industry and Security",
#     "Entity List (EL) - Bureau of Industry and Security",
#     "Denied Persons List (DPL) - Bureau of Industry and Security",
#     "Military End User (MEU) List - Bureau of Industry and Security",

# The following programs refer to entities that are emitted with the ID format
# `ofac-<entity_number>`. This allows them to match directly with existing OFAC data.
# It prevents duplication across datasets.
#
#     "Capta List (CAP) - Treasury Department",
#     "Palestinian Legislative Council List (PLC) - Treasury Department",
#     "Foreign Sanctions Evaders (FSE) - Treasury Department",
#     "Non-SDN Chinese Military-Industrial Complex Companies List (CMIC) - Treasury Department",
#     "Non-SDN Menu-Based Sanctions List (NS-MBS List) - Treasury Department",
#     "Specially Designated Nationals (SDN) - Treasury Department",
#     "Sectoral Sanctions Identifications List (SSI) - Treasury Department",

REGEX_AUTHORITY_ID_SEP = re.compile(r"(\d+ F\.?R\.?)")


def lookup_topic(context: Context, program_name: Optional[str]) -> Optional[str]:
    """Lookup the topic based on the sanction program."""
    res = context.lookup("sanction.program", program_name)
    if res is None:
        context.log.warn(f"Topic for {program_name!r} not found.")
        return None
    return res.topic


def emit_relationship(
    context: Context,
    entity_id: str,
    *,
    related_name: str,
    list_entry: Dict[str, Any],
    source_program: str,
):
    related_entity = context.make("LegalEntity")
    related_entity.id = context.make_id(related_name)
    related_entity.add("name", related_name)
    context.emit(related_entity)

    make_and_emit_sanction(
        context,
        related_entity,
        source_program=source_program,
        list_entry=list_entry,
    )

    rel = context.make("UnknownLink")
    rel.id = context.make_id(entity_id, "linked to", rel.id)
    rel.add("subject", entity_id)
    rel.add("object", related_entity.id)
    context.emit(rel)


def make_and_emit_sanction(
    context: Context, entity: Entity, *, source_program: str, list_entry: Dict[str, Any]
):
    sanction = h.make_sanction(
        context,
        entity,
        program_name=source_program,
        source_program_key=source_program,
        program_key=(
            h.lookup_sanction_program_key(context, source_program)
            if source_program
            else None
        ),
    )
    sanction.add("program", list_entry.pop("programs", []))
    sanction.add("provisions", list_entry.pop("license_policy", []))
    sanction.add("reason", list_entry.pop("license_requirement", []))
    sanction.add(
        "authorityId",
        clean_authority(list_entry.pop("federal_register_notice", None)),
    )
    h.apply_date(sanction, "startDate", list_entry.pop("start_date", None))
    h.apply_date(sanction, "endDate", list_entry.pop("end_date", None))
    sanction.add("country", "us")
    sanction.add("authority", source_program)
    sanction.add("sourceUrl", list_entry.pop("source_information_url", None))

    context.emit(sanction)


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


def parse_list_entry(context: Context, list_entry: Dict[str, Any]):
    for k, v in list(list_entry.items()):
        if isinstance(v, str) and len(v.strip()) == 0:
            list_entry.pop(k)

    type_ = list_entry.pop("type", None)
    schema = context.lookup_value("type", type_)
    if schema is None:
        context.log.error("Unknown result type", type=type_)
        return
    name = list_entry.pop("name", None)
    if name and re.match(r"^Address \d+", name):
        schema = "Address"

    entity = context.make(schema)
    entity.id = context.make_slug(list_entry.pop("id"))
    source_program = list_entry.pop("source", "")
    if source_program:
        entity.add("topics", lookup_topic(context, source_program))
    else:
        # Fallback for entities that don't have a program explicitly stated
        entity.add("topics", "sanction")
    entity_number = list_entry.pop("entity_number", None)
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
    if "bis.doc.gov" in list_entry.get("source_list_url", ""):
        name = name.replace("?s ", "'s ")
        name = name.replace("?", " ")

    name_with_info_res = context.lookup("name_with_info_res", name)
    if name_with_info_res is not None:
        entity.add("name", name_with_info_res.properties["name"], original_value=name)
        entity.add("previousName", name_with_info_res.properties.get("previous_name"))
        entity.add("notes", name_with_info_res.properties.get("notes"))

        related_names = name_with_info_res.properties.get("related")
        # For now we only support one related name, even though conceptually there could be multiple
        assert isinstance(related_names, str) or related_names is None
        # Example: "Huawei Technologies Co., Ltd. (Huawei). Affiliated Entity: Hangzhou Huawei (...)"
        # Here, the affiliate (Hangzhou Huawei) is processed directly, while the main entity (Huawei)
        # is extracted from the 'related' field and emitted via 'emit_relationship'. Since some main
        # entities might only appear through related names, we also apply 'make_and_emit_sanction'
        # within 'emit_relationship' to ensure they're not missed.
        if related_names:
            emit_relationship(
                context,
                entity.id,
                related_name=related_names[0],
                list_entry=list_entry,
                source_program=source_program,
            )
    else:
        # If it's a really long name, it's likely a name with extra info
        if len(name) > registry.name.max_length or any(
            kw in name.lower() for kw in ["affiliated", "subordinate", "f.k.a."]
        ):
            context.log.warning(
                "Name long is very long, maybe it contains extra information?",
                name=name,
            )
        entity.add("name", name)

    if is_ofac:
        context.emit(entity)
        # Don't double-import OFAC entities
        return

    for alias in ensure_list(list_entry.pop("alt_names", "")):
        entity.add("alias", alias.split("; "))
    entity.add("notes", list_entry.pop("remarks", None))
    entity.add("country", list_entry.pop("country", None))
    if entity.schema.is_a("Person"):
        entity.add("position", list_entry.pop("title", None))
        entity.add("nationality", list_entry.pop("nationalities", None))
        entity.add("citizenship", list_entry.pop("citizenships", None))
        for dob in ensure_list(list_entry.pop("dates_of_birth", "")):
            dob = h.multi_split(dob, ["circa ", " to "])
            for date in dob:
                h.apply_date(entity, "birthDate", date)
        entity.add("birthPlace", list_entry.pop("places_of_birth", None))
    elif entity.schema.is_a("Vessel"):
        entity.add("flag", list_entry.pop("vessel_flag", None))
        entity.add("callSign", list_entry.pop("call_sign", None))
        entity.add("type", list_entry.pop("vessel_type", None))
        grt = list_entry.pop("gross_registered_tonnage", None)
        entity.add("grossRegisteredTonnage", grt)
        gt = list_entry.pop("gross_tonnage", None)
        entity.add("tonnage", gt)

        # TODO: make adjacent owner entity
        list_entry.pop("vessel_owner", None)

    assert list_entry.pop("title", None) is None
    # print(result)
    assert not list_entry.pop("nationalities", None)
    assert not list_entry.pop("citizenships", None)
    assert not list_entry.pop("dates_of_birth", None)
    assert not list_entry.pop("places_of_birth", None)
    list_entry.pop("call_sign", None)
    assert not list_entry.pop("gross_tonnage", None)
    assert not list_entry.pop("gross_registered_tonnage", None)
    assert not list_entry.pop("vessel_flag", None)
    assert not list_entry.pop("vessel_owner", None)
    assert not list_entry.pop("vessel_type", None)

    for obj in parse_addresses(context, list_entry.pop("addresses", [])):
        # h.apply_address(context, entity, obj)
        if entity.schema.is_a("Address"):
            entity.add("full", obj.get("full"))
        else:
            h.copy_address(entity, obj)

    for ident in list_entry.pop("ids", []):
        context.log.warning("Unknown ID type", id=ident)

    make_and_emit_sanction(
        context,
        entity,
        source_program=source_program,
        list_entry=list_entry,
    )
    list_entry.pop("source_list_url")

    context.emit(entity)
    context.audit_data(list_entry, ignore=["standard_order"])


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
        for list_entry in data.get("results"):
            parse_list_entry(context, list_entry)
