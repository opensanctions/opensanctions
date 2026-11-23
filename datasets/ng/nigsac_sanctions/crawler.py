import json
from datetime import date, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

from normality import slugify
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

API_URL = "https://nigsac.gov.ng/api/v1/"
SOURCE = "NG-NIGSAC"
IDENTIFIER_PROPS = {
    "Nin": "idNumber",
    "Bvn": "idNumber",
    "DriversLicence": "idNumber",
    "Passport": "passportNumber",
    "RcNumber": "registrationNumber",
}


def parse_page(context: Context, url: str) -> dict[str, str | None]:
    doc = context.fetch_html(url, cache_days=1)
    data: dict[str, str | None] = {}
    for dt in doc.findall(".//dt"):
        key = slugify(h.element_text(dt))
        next = dt.getnext()
        if next is None:
            value = None
        elif next.tag == "dt":
            value = None
        elif next.tag == "dd":
            value = h.element_text(next) or None
        else:
            context.log.warning("Unexpected tag after key", key=key, tag=next, url=url)
            value = None
        if key is not None:
            data[key] = value
    return data


def fetch_modified(context: Context) -> dict[int, str]:
    """Map each API entry ID to the time its latest change was detected."""
    # The feed starts with NFIU's first import in September 2026, and the API
    # rejects ranges longer than ten years.
    tomorrow = date.today() + timedelta(days=1)
    params = {"from": "2026-09-01", "to": tomorrow.isoformat()}
    changes = context.fetch_json(API_URL + "changes", params=params)
    assert changes["truncated"] is False, changes["counts"]
    modified: dict[int, str] = {}
    for item in changes["items"]:
        if item["source"] != SOURCE:
            continue
        entry_id = item["entryId"]
        modified[entry_id] = max(modified.get(entry_id, ""), item["detectedUtc"])
    return modified


def apply_identifiers(
    context: Context, entity: Entity, identifiers: list[dict[str, Any]]
) -> None:
    for ident in identifiers:
        id_type = ident["type"]
        if id_type == "Other" and ident["identifierTypeRaw"] == "Paspport":
            id_type = "Passport"
        prop = IDENTIFIER_PROPS.get(id_type)
        if prop is None:
            raise ValueError(f"Unknown identifier type: {ident}")
        entity.add(prop, ident["valueRaw"])


def crawl_record(context: Context, record: dict[str, Any], modified: str) -> None:
    attrs: dict[str, list[str]] = {}
    for attr in record["attributes"]:
        attrs.setdefault(attr["type"], []).append(attr["value"])
    (url,) = attrs.pop("SourceUrl")
    (program,) = attrs.pop("Programme")
    assert program == "Nigeria Sanctions List", program
    data = parse_page(context, url)
    ref = record["referenceNumber"]
    entry_type = record["entryType"]

    if entry_type == "Individual":
        entity = context.make("Person")
        # Reference numbers are reused across designation batches, so the portal's
        # record ID is needed to tell the designees apart.
        (record_id,) = parse_qs(urlparse(url).query)["id"]
        entity.id = context.make_slug(ref, record_id)
        h.apply_name(
            entity,
            first_name=data.pop("first-name"),
            middle_name=data.pop("middlename"),
            last_name=data.pop("surname"),
        )
        h.apply_date(entity, "birthDate", data.pop("date-of-birth"))
        birth_place = h.format_address(
            city=data.pop("birth-city"), country=data.pop("birth-country")
        )
        entity.add("birthPlace", birth_place or None)
        entity.add("nationality", data.pop("nationality"))
    elif entry_type == "Entity":
        entity = context.make("LegalEntity")
        # Groups are listed on individual pages, with their name split into
        # person name parts.
        name = data.pop("entity-name", None) or record["displayName"]
        res = context.lookup("group_names", name)
        if res is not None:
            name = res.props["entity-name"]
        entity.id = context.make_id(name)
        entity.add("name", name)
        if res is not None:
            entity.add("alias", res.props.get("aliases"))
            entity.add("country", res.props["country"])
        entity.add("country", data.pop("country", None))
        entity.add("country", data.pop("nationality", None))
        h.apply_date(entity, "incorporationDate", data.pop("incorporation-date", None))
        entity.add("registrationNumber", data.pop("incorporation-number", None))
        page_ref = data.pop("referance-number", None)
        if page_ref is not None and page_ref != ref:
            context.log.warning("Reference number mismatch", page=page_ref, api=ref)
    else:
        raise ValueError(f"Unknown entry type: {entry_type}")

    entity.add("alias", data.pop("aliases", None))
    address = data.pop("address", None)
    entity.add("address", address.removeprefix("- ") if address else address)
    entity.add("notes", data.pop("comments", None))
    entity.add("notes", data.pop("narrative-summary"))
    for phones in (data.pop("phone-number", None), data.pop("phone-numbers", None)):
        phones = phones.replace("- ", "") if phones else phones
        entity.add("phone", h.multi_split(phones, [","]))
    entity.add("sourceUrl", url)
    entity.add("topics", "sanction")

    for name_obj in record["names"]:
        if name_obj["type"] == "Primary":
            continue
        if name_obj["type"] != "LowAlias":
            raise ValueError(f"Unknown name type: {name_obj}")
        if name_obj["fullNameRaw"] == "(FNU)":
            continue
        entity.add("alias", name_obj["fullNameRaw"])
    apply_identifiers(context, entity, record["identifiers"])
    for addr in record["addresses"]:
        if addr["kind"] == "PlaceOfBirth":
            continue
        elif addr["kind"] == "Address":
            address = h.format_address(
                street=addr["street"],
                city=addr["city"],
                state=addr["stateProvince"],
                country=addr["country"],
            )
            entity.add("address", address)
            note = addr["note"]
            if note is not None and not note.startswith("Alternate: "):
                raise ValueError(f"Unknown address note: {note}")
            if note is not None:
                entity.add("address", note.removeprefix("Alternate: "))
        else:
            raise ValueError(f"Unknown address kind: {addr}")
    entity.add("phone", h.multi_split(attrs.pop("PhoneNumber", []), [","]))

    key = ref if entry_type == "Entity" else None
    sanction = h.make_sanction(context, entity, key=key)
    sanction.add("authorityId", ref)
    sanction.add("reason", data.pop("reason-for-designation"))
    h.apply_date(sanction, "startDate", data.pop("sanction-date"))
    h.apply_date(sanction, "endDate", record["delistedOn"])
    h.apply_date(sanction, "modifiedAt", modified)
    h.apply_date(sanction, "listingDate", data.pop("record-date"))
    sanction.add("description", data.pop("press-release"))

    context.emit(entity)
    context.emit(sanction)
    # The page carries these verbatim, while the API holds NFIU's parse of them.
    for parsed in ("ReasonForListing", "NarrativeSummary", "Comment", "Nationality"):
        attrs.pop(parsed, None)
    if len(attrs):
        context.log.warning("Unhandled attributes", attrs=attrs, url=url)
    context.audit_data(data, ["first-name", "middlename", "surname"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("register.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path) as fh:
        register = json.load(fh)
    assert register["pages"] == 1, register["pages"]
    modified = fetch_modified(context)

    for row in register["rows"]:
        subject = context.fetch_json(API_URL + "subjects/" + row["key"], cache_days=1)
        (record,) = [r for r in subject["records"] if r["source"] == SOURCE]
        crawl_record(context, record, modified[record["id"]])
