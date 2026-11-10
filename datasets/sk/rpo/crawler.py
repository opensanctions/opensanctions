import gzip
import json
import re
from collections import Counter
from collections.abc import Generator
from pathlib import Path
from typing import Any

from lxml import etree
from normality import slugify

from zavod import Context, Entity
from zavod import helpers as h

ICO = re.compile(r"^\d{8}$")
LIST_FILE = re.compile(r"^batch-init/init_(\d{4}-\d{2}-\d{2})_list\.txt$")
COMMERCIAL_REGISTER = "1"
# The source uses this date when the real one is not known.
UNKNOWN_DATE = "1800-01-01"

Record = dict[str, Any]


def make_company_id(ico: str) -> str:
    return f"oc-companies-sk-{ico}"


def current(values: list[Record]) -> list[Record]:
    return [v for v in values if v.get("validTo") is None]


def clean_name(name: str) -> str:
    # Doubled angle brackets are ASCII stand-ins for guillemets, e.g. OOO firma <<MARITANA>>.
    return name.replace("<<", "«").replace(">>", "»")


def join_names(names: list[str] | None) -> str | None:
    if not names:
        return None
    return " ".join(names)


def make_address(
    context: Context, addr: Record | None
) -> tuple[str | None, str | None]:
    """Format an RPO address and resolve its country to an ISO code."""
    if addr is None:
        return None, None
    country: str | None = None
    country_data = addr.pop("country", None)
    if country_data is not None:
        res = context.lookup("country", country_data.get("code"))
        if res is None:
            context.log.warning(
                "Unknown country code",
                code=country_data.get("code"),
                value=country_data.get("value"),
            )
        else:
            country = res.value
    street = addr.pop("street", None)
    municipality = addr.pop("municipality", None)
    city: str | None = None
    summary: str | None = None
    if municipality is not None:
        # Without a codelist code or a street, the municipality field holds a
        # free-text address.
        if municipality.get("code") is None and street is None:
            summary = municipality.get("value")
        else:
            city = municipality.get("value")
    reg_number = addr.pop("regNumber", None)
    numbers = [
        str(reg_number) if reg_number else None,
        addr.pop("buildingNumber", None),
    ]
    house_number = "/".join(n for n in numbers if n) or None
    postal_codes = addr.pop("postalCodes", None) or []
    addr.pop("validFrom", None)
    addr.pop("validTo", None)
    addr.pop("buildingIndex", None)
    context.audit_data(addr, ignore=["formatedAddress", "districts"])
    address = h.format_address(
        summary=summary,
        street=street,
        house_number=house_number,
        postal_code=", ".join(postal_codes) or None,
        city=city,
        country_code=country,
    )
    if slugify(address) is None:
        return None, country
    return address, country


def emit_person(context: Context, owner_id: str, party: Record) -> tuple[str, str]:
    """Emit a natural person party; return its entity ID and name key."""
    person_name: Record = party.pop("personName")
    first_name = join_names(person_name.pop("givenNames", None))
    last_name = join_names(person_name.pop("familyNames", None))
    maiden_name = join_names(person_name.pop("givenFamilyNames", None))
    formatted = person_name.pop("formatedName", None)
    name = h.make_name(first_name=first_name, last_name=last_name) or formatted
    if name is None:
        raise ValueError(f"Person without a name: {party!r}")
    address, country = make_address(context, party.pop("address", None))

    person_id = context.make_id("person", name, address or owner_id)
    assert person_id is not None
    person = context.make("Person")
    person.id = person_id
    h.apply_name(
        person,
        full=None if first_name or last_name else name,
        first_name=first_name,
        last_name=last_name,
        maiden_name=maiden_name,
    )
    for title in person_name.pop("prefixes", []) + person_name.pop("postfixes", []):
        person.add("title", title.get("value"))
    person.add("address", address)
    person.add("country", country)
    context.audit_data(person_name)
    context.emit(person)
    return person_id, name


def emit_legal_entity(context: Context, party: Record) -> tuple[str, str]:
    """Emit a stub for a legal-entity party or successor; return its ID and name."""
    name = clean_name(party.pop("fullName"))
    identifier = party.pop("identifier", None)
    address, country = make_address(context, party.pop("address", None))

    # Foreign parties carry the placeholder "Neuvedené" (not stated).
    has_ico = identifier is not None and ICO.match(identifier) is not None
    if has_ico:
        entity_id: str | None = make_company_id(identifier)
    else:
        entity_id = context.make_id("company", name, country)
    assert entity_id is not None
    entity = context.make("LegalEntity")
    entity.id = entity_id
    if has_ico:
        entity.add("registrationNumber", identifier)
        entity.add("jurisdiction", "sk")
    entity.add("name", name)
    entity.add("country", country)
    # A Slovak company's own record carries its address; a party entry holds a
    # snapshot that may be long outdated.
    if not has_ico:
        entity.add("address", address)
    establishment = party.pop("establishment", None)
    if establishment != UNKNOWN_DATE:
        entity.add("incorporationDate", establishment)
    entity.add("dissolutionDate", party.pop("termination", None))
    context.emit(entity)
    return entity_id, name


def make_relation(
    context: Context, schema: str, company_id: str, party_id: str, party: Record
) -> Entity:
    type_data: Record = party.pop("stakeholderType")
    member = party.pop("statutoryBodyMember", None)
    start = party.pop("validFrom", None)
    end = party.pop("validTo", None)

    rel = context.make(schema)
    # Statutory body and stakeholder types come from codelists with overlapping codes.
    codelist, code = type_data["codelistCode"], type_data["code"]
    rel.id = context.make_id(company_id, codelist, code, party_id, start)
    if schema == "Directorship":
        rel.add("director", party_id)
        rel.add("organization", company_id)
    elif schema == "Ownership":
        rel.add("owner", party_id)
        rel.add("asset", company_id)
    elif schema == "Representation":
        rel.add("agent", party_id)
        rel.add("client", company_id)
    else:
        raise ValueError(f"Unknown relation schema: {schema}")
    rel.add("role", type_data["value"].strip())
    if member is not None:
        rel.add("role", member.get("value"))
    if start != UNKNOWN_DATE:
        rel.add("startDate", start)
    rel.add("endDate", end)
    return rel


def crawl_parties(
    context: Context, company_id: str, record: Record, deposit_stats: Counter[str]
) -> None:
    # Current ownerships by party name, to attach the partners' current deposits.
    owners: dict[str, list[Entity]] = {}
    relations: list[Entity] = []
    parties = [("Directorship", p) for p in record.pop("statutoryBodies", [])]
    for party in record.pop("stakeholders", []):
        type_value = party["stakeholderType"]["value"]
        res = context.lookup("stakeholder_type", type_value)
        if res is None:
            raise ValueError(f"Unknown stakeholder type: {type_value!r}")
        if res.value != "skip":
            parties.append((str(res.value), party))
    for schema, party in parties:
        if party.get("personName") is not None:
            party_id, name = emit_person(context, company_id, party)
        elif party.get("fullName") is not None:
            party_id, name = emit_legal_entity(context, party)
        else:
            raise ValueError(f"Party without a name: {party!r}")
        is_current = party.get("validTo") is None
        rel = make_relation(context, schema, company_id, party_id, party)
        context.audit_data(party)
        relations.append(rel)
        if rel.schema.name == "Ownership" and is_current:
            owners.setdefault(name, []).append(rel)

    for deposit in current(record.pop("deposits", [])):
        deposit_name: str | None = deposit.get("fullName")
        if deposit_name is not None:
            deposit_name = clean_name(deposit_name)
        person_name = deposit.get("personName")
        if person_name is not None:
            deposit_name = h.make_name(
                first_name=join_names(person_name.get("givenNames")),
                last_name=join_names(person_name.get("familyNames")),
            )
        matches = owners.get(deposit_name or "", [])
        if len(matches) != 1:
            deposit_stats["unmatched"] += 1
            continue
        deposit_stats["matched"] += 1
        matches[0].add("sharesValue", deposit.get("amount"))
        matches[0].add("sharesCurrency", deposit.get("currency", {}).get("code"))

    for rel in relations:
        context.emit(rel)


def emit_succession(
    context: Context, company_id: str, other: Record, company_is_successor: bool
) -> None:
    start = other.pop("validFrom", None)
    other.pop("validTo", None)
    other_id, _ = emit_legal_entity(context, other)
    context.audit_data(other)

    succ = context.make("Succession")
    if company_is_successor:
        predecessor, successor = other_id, company_id
    else:
        predecessor, successor = company_id, other_id
    succ.id = context.make_id("succession", predecessor, successor)
    succ.add("predecessor", predecessor)
    succ.add("successor", successor)
    succ.add("startDate", start)
    context.emit(succ)


def crawl_record(context: Context, record: Record, deposit_stats: Counter[str]) -> None:
    identifiers = record.pop("identifiers")
    current_icos = current(identifiers)
    if len(current_icos) != 1:
        raise ValueError(f"Expected one current IČO: {identifiers!r}")
    ico = current_icos[0]["value"]

    legal_form = current(record.pop("legalForms"))[-1]["value"]["value"]
    res = context.lookup("legal_form", legal_form)
    if res is None:
        raise ValueError(f"Unknown legal form: {legal_form!r}")

    # Some branches have the placeholder "Neuvedené" (not stated) instead of an IČO.
    if ICO.match(ico):
        entity_id: str | None = make_company_id(ico)
    else:
        entity_id = context.make_slug(str(record.pop("id")))
    assert entity_id is not None
    company = context.make(res.schema)
    company.id = entity_id
    company.add("jurisdiction", "sk")
    company.add("legalForm", legal_form)
    for identifier in identifiers:
        if ICO.match(identifier["value"]):
            company.add("registrationNumber", identifier["value"])
    for name in record.pop("fullNames"):
        prop = "name" if name.get("validTo") is None else "previousName"
        company.add(prop, clean_name(name["value"]))
    for name in record.pop("alternativeNames", []):
        company.add("alias", clean_name(name["value"]))
    company.add("incorporationDate", record.pop("establishment", None))
    for status in current(record.pop("legalStatuses", [])):
        company.add("status", status["value"]["value"])

    source = record.pop("sourceRegister")
    for number in current(source.get("registrationNumbers", [])):
        company.add("registrationNumber", number["value"])

    seats = current(record.pop("addresses"))
    for seat in seats:
        address, country = make_address(context, seat)
        company.add("address", address)
        company.add("country", country)

    stat_codes = record.pop("statisticalCodes", None)
    if stat_codes is not None and "mainActivity" in stat_codes:
        company.add("sector", stat_codes["mainActivity"]["value"])

    capital = [e for e in current(record.pop("equities", [])) if "value" in e]
    if len(capital) == 1 and company.schema.is_a("Company"):
        company.add("capital", capital[0]["value"])

    crawl_parties(context, entity_id, record, deposit_stats)
    for other in record.pop("predecessors", []):
        emit_succession(context, entity_id, other, company_is_successor=True)
    for other in record.pop("successors", []):
        emit_succession(context, entity_id, other, company_is_successor=False)

    context.emit(company)
    context.audit_data(
        record,
        ignore=[
            "id",
            "containsInconsistency",
            "activities",
            "authorizations",
            "otherLegalFacts",
            "shares",
            "organizationUnits",
        ],
    )


def read_records(path: Path, export_date: str) -> Generator[Record, None, None]:
    """Stream the records of an export file, which holds one record per line."""
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        header = json.loads(fh.readline().strip() + "]}")
        if header["exportDate"] != export_date:
            raise ValueError(f"Unexpected export date in {path.name}: {header!r}")
        for line in fh:
            line = line.strip()
            if line == "]}":
                return
            if line:
                yield json.loads(line.removeprefix(","))
    raise ValueError(f"Truncated export file: {path.name}")


def crawl(context: Context) -> None:
    listing = context.fetch_text(f"{context.data_url}?list-type=2&prefix=batch-init/")
    if listing is None:
        raise ValueError("Empty bucket listing")
    doc = etree.fromstring(listing.encode("utf-8"))
    if [el.text for el in doc.iter("{*}IsTruncated")] != ["false"]:
        raise ValueError("Bucket listing is truncated")
    keys = {el.text for el in doc.iter("{*}Key") if el.text is not None}

    batches = sorted(m.group(1) for k in keys if (m := LIST_FILE.match(k)))
    if not batches:
        raise ValueError("No complete batch-init export found")
    export_date = batches[-1]
    file_list = context.fetch_text(
        f"{context.data_url}batch-init/init_{export_date}_list.txt"
    )
    if file_list is None:
        raise ValueError("Empty batch file list")
    file_names = file_list.split()
    for file_name in file_names:
        if f"batch-init/{file_name}" not in keys:
            raise ValueError(f"Batch file missing from bucket: {file_name}")
    context.log.info("Export batch", date=export_date, files=len(file_names))

    deposit_stats: Counter[str] = Counter()
    seen = 0
    for file_name in file_names:
        url = f"{context.data_url}batch-init/{file_name}"
        path = context.fetch_resource(file_name, url)
        for record in read_records(path, export_date):
            seen += 1
            if seen % 50_000 == 0:
                context.log.info("Processed records", seen=seen, file=file_name)
            source = record["sourceRegister"]["value"]["code"]
            if source != COMMERCIAL_REGISTER or record.get("termination") is not None:
                continue
            crawl_record(context, record, deposit_stats)
    context.log.info("Deposits attached to ownerships", **deposit_stats)
