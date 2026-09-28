"""Hong Kong targeted financial sanctions lists published by the CEDB.

The Commerce and Economic Development Bureau publishes one XML file per UN
regime on data.gov.hk. Each file is a snapshot of part of the UN Security
Council consolidated list, in the UN's own schema, and the snapshots lag the
UN list. We cross-check every record against the live UN list and skip those
the UN has since delisted, so that a stale snapshot never re-flags them.
"""

import re

from lxml.etree import _Element as Element
from normality import squash_spaces
from rigour.mime.types import XML
from zavod.shed.un_sc import get_legal_entities, get_persons, load_un_sc

from zavod import Context, Entity
from zavod import helpers as h

# Child elements of each record type. Anything outside these sets is new to the
# source and is reported, so it can be mapped rather than silently dropped.
# LAST_REVIEWED_ON, the INTERPOL links, SORT_KEY* and the boolean flags
# (DESIG, ADDRESS, GOODQUALITY, PASSPORT) carry nothing we publish. LIST_TYPE
# and VERSIONNUM are constant.
COMMON_TAGS = {
    "DATAID",
    "VERSIONNUM",
    "FIRST_NAME",
    "UN_LIST_TYPE",
    "REFERENCE_NUMBER",
    "LISTED_ON",
    "COMMENTS1",
    "NAME_ORIGINAL_SCRIPT",
    "LIST_TYPE",
    "LAST_DAY_UPDATED",
    "LAST_REVIEWED_ON",
    "SORT_KEY",
    "SORT_KEY_LAST_MOD",
    "HAS_INTERPOL_LINK",
    "INTERPOL_LINK",
}
INDIVIDUAL_TAGS = COMMON_TAGS | {
    "SECOND_NAME",
    "THIRD_NAME",
    "FOURTH_NAME",
    "GENDER",
    "TITLE",
    "DESIGNATION",
    "NATIONALITY",
    "INDIVIDUAL_ALIAS",
    "INDIVIDUAL_ADDRESS",
    "INDIVIDUAL_DATE_OF_BIRTH",
    "INDIVIDUAL_PLACE_OF_BIRTH",
    "INDIVIDUAL_DOCUMENT",
    "DESIG",
    "ADDRESS",
    "GOODQUALITY",
    "PASSPORT",
}
ENTITY_TAGS = COMMON_TAGS | {"ENTITY_ALIAS", "ENTITY_ADDRESS"}
ADDRESS_TAGS = {"STREET", "CITY", "STATE_PROVINCE", "ZIP_CODE", "COUNTRY", "NOTE"}
# Year spans such as "1984 - 1986" in alias birth dates.
YEAR_RANGE = re.compile(r"^(\d{4})\s*-\s*(\d{4})$")
# The widest birth-year span we expand into individual candidate years.
MAX_YEAR_SPAN = 10


def text(node: Element, path: str) -> str | None:
    value = node.findtext(path)
    if value is None:
        return None
    return squash_spaces(value) or None


def values(node: Element, path: str) -> list[str]:
    """Return the non-empty VALUE children of the element at `path`."""
    out: list[str] = []
    for child in node.findall(f"{path}/VALUE"):
        if child.text is not None and child.text.strip():
            out.append(squash_spaces(child.text))
    return out


def audit_tags(context: Context, node: Element, known: set[str]) -> None:
    unknown = {str(child.tag) for child in node} - known
    if unknown:
        context.log.warning(
            "Unknown elements in record",
            parent=node.tag,
            tags=sorted(unknown),
            ref=text(node, "./REFERENCE_NUMBER"),
        )


def apply_year_range(
    context: Context, person: Entity, start: str | None, end: str | None
) -> None:
    """Add every year of a birth-year span as a candidate birth date."""
    original = f"{start} - {end}"
    if start is None or end is None or not start.isdigit() or not end.isdigit():
        context.log.warning("Invalid birth year range", value=original, id=person.id)
        return
    first, last = int(start), int(end)
    if not 0 < last - first <= MAX_YEAR_SPAN:
        context.log.warning(
            "Implausible birth year range", value=original, id=person.id
        )
        return
    for year in range(first, last + 1):
        person.add("birthDate", str(year), original_value=original)


def apply_birth_date(context: Context, person: Entity, value: str | None) -> None:
    if value is None:
        return
    match = YEAR_RANGE.match(value)
    if match is not None:
        apply_year_range(context, person, match.group(1), match.group(2))
        return
    h.apply_date(person, "birthDate", value)


def make_address(context: Context, node: Element) -> Entity | None:
    audit_tags(context, node, ADDRESS_TAGS)
    postal_code = text(node, "./ZIP_CODE")
    region = text(node, "./STATE_PROVINCE")
    # Some records put a region name in the ZIP_CODE field.
    if postal_code is not None and not re.search(r"\d", postal_code):
        region = postal_code if region is None else f"{region}, {postal_code}"
        postal_code = None
    return h.make_address(
        context,
        remarks=text(node, "./NOTE"),
        street=text(node, "./STREET"),
        city=text(node, "./CITY"),
        region=region,
        postal_code=postal_code,
        country=text(node, "./COUNTRY"),
    )


def parse_alias(context: Context, entity: Entity, node: Element) -> None:
    audit_tags(
        context,
        node,
        {
            "QUALITY",
            "ALIAS_NAME",
            "NOTE",
            "DATE_OF_BIRTH",
            "CITY_OF_BIRTH",
            "COUNTRY_OF_BIRTH",
        },
    )
    # Birth details recorded against an alias identity are further candidates
    # for the person's own. They occur even on alias records without a name.
    # The free-text NOTE ("previously listed as", original script) is not kept.
    if entity.schema.is_a("Person"):
        apply_birth_date(context, entity, text(node, "./DATE_OF_BIRTH"))
        entity.add("birthPlace", text(node, "./CITY_OF_BIRTH"))
        entity.add("birthCountry", text(node, "./COUNTRY_OF_BIRTH"))

    name = text(node, "./ALIAS_NAME")
    if name is None:
        return
    quality = text(node, "./QUALITY")
    prop = context.lookup_value("alias_quality", quality)
    if prop is None:
        context.log.warning("Unknown alias quality", quality=quality, name=name)
        return
    for part in name.split("; "):
        entity.add(prop, part)


def parse_birth_date(context: Context, person: Entity, node: Element) -> None:
    audit_tags(
        context, node, {"TYPE_OF_DATE", "DATE", "YEAR", "FROM_YEAR", "TO_YEAR", "NOTE"}
    )
    # TYPE_OF_DATE (EXACT, APPROXIMATELY, BETWEEN) and the NOTE qualify the
    # precision of the date; birthDate cannot carry that, so they are dropped.
    start = text(node, "./FROM_YEAR")
    end = text(node, "./TO_YEAR")
    if start is not None or end is not None:
        apply_year_range(context, person, start, end)
    h.apply_date(person, "birthDate", text(node, "./DATE"))
    h.apply_date(person, "birthDate", text(node, "./YEAR"))


def parse_birth_place(context: Context, person: Entity, node: Element) -> None:
    address = make_address(context, node)
    if address is not None:
        person.add("birthPlace", address.get("full"))
        person.add("birthCountry", address.get("country"))
    elif (note := text(node, "./NOTE")) is not None:
        # A handful of places of birth are given only as a note, e.g.
        # "possibly Sabratha, Talil neighbourhood".
        person.add("birthPlace", note)


def parse_document(context: Context, person: Entity, node: Element) -> None:
    audit_tags(
        context,
        node,
        {
            "TYPE_OF_DOCUMENT",
            "TYPE_OF_DOCUMENT2",
            "NUMBER",
            "ISSUING_COUNTRY",
            "COUNTRY_OF_ISSUE",
            "DATE_OF_ISSUE",
            "DATE_OF_EXPIRY",
            "CITY_OF_ISSUE",
            "NOTE",
        },
    )
    if len(node) == 0:
        return
    doc_type = text(node, "./TYPE_OF_DOCUMENT")
    number = text(node, "./NUMBER")
    result = context.lookup("document_type", doc_type)
    if result is None:
        context.log.warning(
            "Unknown document type", doc_type=doc_type, number=number, id=person.id
        )
        return
    note = text(node, "./NOTE")
    doc_type2 = text(node, "./TYPE_OF_DOCUMENT2")
    country = text(node, "./ISSUING_COUNTRY")
    if number is None:
        # Some records give the number only inside the free text of the note
        # or the secondary document type.
        free_text = " ".join(t for t in (doc_type2, note) if t is not None)
        extracted = context.lookup("document_number", free_text)
        if extracted is None:
            context.log.warning(
                "Document without number", free_text=free_text, id=person.id
            )
            return
        if extracted.value is None:
            return
        number = extracted.value
        country = extracted.country or country
    city = text(node, "./CITY_OF_ISSUE")
    summary = [note, None if city is None else f"Issued in {city}"]
    ident = h.make_identification(
        context,
        person,
        number=number,
        doc_type=doc_type,
        country=country,
        summary="; ".join(s for s in summary if s is not None) or None,
        start_date=text(node, "./DATE_OF_ISSUE"),
        end_date=text(node, "./DATE_OF_EXPIRY"),
        passport=result.passport,
    )
    if ident is None:
        return
    ident.add("country", text(node, "./COUNTRY_OF_ISSUE"))
    ident.add("type", doc_type2)
    context.emit(ident)


def is_current(context: Context, node: Element, un_refs: set[str]) -> bool:
    """Check that the record is still on the live UN consolidated list."""
    ref = text(node, "./REFERENCE_NUMBER")
    if ref is None:
        raise ValueError(f"Record without reference number: {text(node, './DATAID')}")
    if ref in un_refs:
        return True
    context.log.warning(
        "Record is no longer on the UN consolidated list, skipping",
        ref=ref,
        name=text(node, "./FIRST_NAME"),
    )
    return False


def parse_common(context: Context, entity: Entity, node: Element) -> Entity:
    entity.add("alias", text(node, "./NAME_ORIGINAL_SCRIPT"))
    entity.add("notes", h.clean_note(node.findtext("./COMMENTS1")))

    list_type = text(node, "./UN_LIST_TYPE")
    listed_on = text(node, "./LISTED_ON")
    sanction = h.make_sanction(
        context,
        entity,
        # The same record can appear in two files (South Sudan entries are
        # repeated in the Sudan file), so key on the regime, not the file.
        key=list_type,
        program_name=context.lookup_value("regulation", list_type),
        source_program_key=list_type,
        program_key=h.lookup_sanction_program_key(context, list_type),
        start_date=listed_on,
    )
    h.apply_date(sanction, "listingDate", listed_on)
    for updated in values(node, "./LAST_DAY_UPDATED"):
        h.apply_date(sanction, "modifiedAt", updated)
    sanction.add("unscId", text(node, "./REFERENCE_NUMBER"))
    return sanction


def parse_individual(context: Context, node: Element, person: Entity) -> None:
    audit_tags(context, node, INDIVIDUAL_TAGS)
    sanction = parse_common(context, person, node)
    person.add("gender", text(node, "./GENDER"))
    person.add("title", values(node, "./TITLE"))
    person.add("position", values(node, "./DESIGNATION"))
    person.add("nationality", values(node, "./NATIONALITY"))
    for alias in node.findall("./INDIVIDUAL_ALIAS"):
        parse_alias(context, person, alias)
    for addr in node.findall("./INDIVIDUAL_ADDRESS"):
        h.copy_address(person, make_address(context, addr))
    for dob in node.findall("./INDIVIDUAL_DATE_OF_BIRTH"):
        parse_birth_date(context, person, dob)
    for pob in node.findall("./INDIVIDUAL_PLACE_OF_BIRTH"):
        parse_birth_place(context, person, pob)
    for doc in node.findall("./INDIVIDUAL_DOCUMENT"):
        parse_document(context, person, doc)
    context.emit(person)
    context.emit(sanction)


def parse_entity(context: Context, node: Element, entity: Entity) -> None:
    audit_tags(context, node, ENTITY_TAGS)
    sanction = parse_common(context, entity, node)
    for alias in node.findall("./ENTITY_ALIAS"):
        parse_alias(context, entity, alias)
    for addr in node.findall("./ENTITY_ADDRESS"):
        h.copy_address(entity, make_address(context, addr))
    context.emit(entity)
    context.emit(sanction)


def crawl_file(
    context: Context, name: str, url: str, title: str, un_refs: set[str]
) -> None:
    path = context.fetch_resource(f"{name}.xml", url)
    context.export_resource(path, XML, title=f"Source data - {title}")
    doc = context.parse_resource_xml(path)
    prefix = context.dataset.prefix
    assert prefix is not None, "Dataset prefix is required"
    for node, person in get_persons(context, prefix, doc):
        if is_current(context, node, un_refs):
            parse_individual(context, node, person)
    for node, entity in get_legal_entities(context, prefix, doc):
        if is_current(context, node, un_refs):
            parse_entity(context, node, entity)


def crawl(context: Context) -> None:
    _, un_doc = load_un_sc(context)
    un_refs = {
        squash_spaces(ref.text)
        for ref in un_doc.findall(".//REFERENCE_NUMBER")
        if ref.text is not None
    }
    if len(un_refs) == 0:
        raise ValueError("No reference numbers found in the UN consolidated list")

    # The dataset page lists one XML resource per regime, each linked twice
    # (details and download).
    page = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    files: dict[str, str] = {}
    for link in h.xpath_elements(page, "//a[contains(@href, '/datagovhk/TFSlists/')]"):
        url = link.get("href")
        if url is None or not url.endswith(".xml"):
            continue
        name = url.rsplit("/", 1)[-1].removesuffix(".xml")
        files[name] = url
    if len(files) == 0:
        raise ValueError(f"No XML resources found on {context.data_url}")

    for name, url in sorted(files.items()):
        title = name.replace("_", " ").title()
        crawl_file(context, name, url, title, un_refs)
