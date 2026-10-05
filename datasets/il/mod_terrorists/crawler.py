import re
from typing import Any

import orjson
from lxml import html
from lxml.etree import _Element as Element
from rigour.mime.types import JSON
from zavod.entity import Entity

from zavod import Context
from zavod import helpers as h

BASE_URL = "https://matal.mod.gov.il"
# Umbraco Content Delivery API behind the site's CMS
API_URL = f"{BASE_URL}/umbraco/delivery/api/v2/content"
LANGS = {"en": "eng", "he": "heb", "ar": "ara"}
SPLITS = [";", "Id Number", "a) ", "b) ", "c) ", " :", "\n", "• "]
HEBREW = re.compile(r"[\u0590-\u05FF]")
ARABIC = re.compile(r"[\u0600-\u06FF]")
# e.g. 'the source states only the year ("2010"), so the day shown is a placeholder.'
PARTIAL_DATE = re.compile(
    r'^the source states only the (?:year|month and year) \("([^"]+)"\)'
)
# Stand-ins for "no date"; a comment explains 9999-09-09 where it's used
NO_DATES = {"9999-09-09", "0001-01-01"}

Item = dict[str, Any]


def fetch_content(context: Context, content_type: str, lang: str) -> dict[str, Item]:
    items: dict[str, Item] = {}
    total = None
    while total is None or len(items) < total:
        data = context.fetch_json(
            API_URL,
            params={
                "filter": f"contentType:{content_type}",
                "sort": "createDate:asc",
                "skip": len(items),
                "take": 1000,
            },
            headers={"Accept-Language": lang},
        )
        total = data["total"]
        if len(data["items"]) == 0:
            break
        for item in data["items"]:
            items[item["id"]] = item
    if len(items) != total:
        raise ValueError(f"Got {len(items)} of {total} {content_type} items ({lang})")
    return items


def fetch_designated(context: Context, content_type: str) -> list[list[Item]]:
    """Fetch designated items, each as a list of its [en, he, ar] culture variants."""
    by_lang = {lang: fetch_content(context, content_type, lang) for lang in LANGS}
    designated: list[list[Item]] = []
    for item_id, item in by_lang["en"].items():
        flag = item["properties"]["isDesignated"]
        if flag is False:
            continue
        if flag is not True:
            context.log.warning("Unexpected designation flag", id=item_id, flag=flag)
            continue
        designated.append([by_lang[lang][item_id] for lang in LANGS])
    path = context.get_resource_path(f"{content_type}.json")
    path.write_bytes(orjson.dumps(designated))
    context.export_resource(
        path, JSON, title=f"{context.SOURCE_TITLE} ({content_type})"
    )
    return designated


def drop_fallbacks(
    variants: list[Item], prop: str
) -> tuple[str | None, str | None, str | None]:
    """Names in [en, he, ar], dropping copies from another language."""
    name_en, name_he, name_ar = [item["properties"][prop] for item in variants]
    if name_en == name_he and HEBREW.search(name_he or "") is not None:
        name_en = None
    if name_ar in (name_en, name_he) and ARABIC.search(name_ar or "") is None:
        name_ar = None
    return name_en, name_he, name_ar


def apply_aliases(entity: Entity, variants: list[Item], prop: str) -> None:
    for item, lang in zip(variants, LANGS.values()):
        for alias in item["properties"][prop] or []:
            entity.add("alias", h.multi_split(alias, SPLITS), lang=lang)


def pop_blocks(
    props: dict[str, Any], key: str, element_type: str
) -> list[dict[str, Any]]:
    """Pop a block list property and return the properties of each block.

    Umbraco block list values look like
    `[{"elementType": "addressBlock", "properties": {"city": ..., ...}}]`.
    Raises if a block isn't of the expected `element_type`. Callers pop the
    fields they use from each block and audit the rest.
    """
    blocks: list[dict[str, Any]] = []
    for block in props.pop(key) or []:
        if block["elementType"] != element_type:
            raise ValueError(f"Unexpected block type in {key}: {block}")
        blocks.append(dict(block["properties"]))
    return blocks


def html_fragment(value: str) -> Element:
    fragment: Element = html.fragment_fromstring(value, create_parent="div")
    return fragment


def html_text(value: str) -> str:
    return h.element_text(html_fragment(value), squash=False).strip()


def parse_comments(value: str | None) -> tuple[dict[str, list[str]], list[str]]:
    """Split comments into labelled values ("<strong>Label:</strong> value") and notes."""
    comments: dict[str, list[str]] = {}
    notes: list[str] = []
    if value is None:
        return comments, notes
    root = html_fragment(value)
    paragraphs = h.xpath_elements(root, "./p")
    if len(paragraphs) == 0:
        notes.append(h.element_text(root))
    for para in paragraphs:
        labels = h.xpath_elements(para, "./strong")
        text = h.element_text(para)
        if len(labels) == 0 or not text.startswith(h.element_text(labels[0])):
            notes.append(text)
            continue
        label = h.element_text(labels[0])
        key = label.rstrip(":").strip()
        comments.setdefault(key, []).append(text[len(label) :].strip())
    return comments, notes


def apply_date(entity: Entity, prop: str, value: str | None) -> None:
    if value is not None and value not in NO_DATES:
        h.apply_date(entity, prop, value)


def apply_partial_date(
    context: Context,
    entity: Entity,
    prop: str,
    dates: list[str | None],
    comments: list[str] | None,
) -> None:
    """Apply dates, or the partial date of a comment saying the day is a placeholder."""
    if comments is None:
        for date in dates:
            apply_date(entity, prop, date)
        return
    for comment in comments:
        match = PARTIAL_DATE.match(comment)
        if match is None:
            context.log.warning("Unexpected date comment", prop=prop, comment=comment)
            continue
        h.apply_date(entity, prop, match.group(1))


def apply_designation(
    context: Context,
    sanction: Entity,
    props: dict[str, Any],
    comments: dict[str, list[str]],
) -> None:
    apply_date(sanction, "startDate", props.pop("temporaryDesignationDate"))
    apply_date(sanction, "startDate", props.pop("permanentDesignationDate"))
    apply_date(sanction, "endDate", props.pop("cancellationDate"))
    for value in comments.pop("West Bank designation date", []):
        h.apply_date(sanction, "startDate", value)
    for key in ("Designation date", "Cancellation date"):
        for comment in comments.pop(key, []):
            if "9/9/9999 is a placeholder" not in comment:
                context.log.warning("Unexpected date comment", key=key, comment=comment)
    # Cancelled without a date given: "הכרזה בוטלה" (designation cancelled)
    for comment in comments.pop("Cancellation", []):
        if comment != "הכרזה בוטלה":
            context.log.warning("Unexpected cancellation comment", comment=comment)
    sanction.add("authority", props.pop("localDesignator"))
    sanction.add("recordId", props.pop("foreignDesignationReferenceNumber"))


def split_ids(value: str | None) -> list[str]:
    if value is None:
        return []
    # Keep labels such as "מס' דרכון סורי:" on the same line as their number
    return h.multi_split(value.replace(":\n", ": "), SPLITS)


def source_url(item: Item) -> str:
    return f"{BASE_URL}{item['route']['path']}"


def apply_address(context: Context, entity: Entity, block: dict[str, Any]) -> None:
    country = block.pop("country")
    if country == "Unknown":
        country = None
    notes = block.pop("notes")
    if notes is not None:
        # Only used to give the country when it's not set, e.g. "Gaza Strip"
        if country is not None:
            context.log.warning("Address note with country", notes=notes, block=block)
        country = html_text(notes)
    address = h.make_address(
        context,
        street=block.pop("street"),
        city=block.pop("city"),
        postal_code=block.pop("postalCode"),
        country=country,
    )
    h.apply_address(context, entity, address)
    entity.add("country", country)
    context.audit_data(block)


def emit_key_operatives(context: Context, entity: Entity, operatives: str) -> None:
    res = context.lookup("key_operatives", operatives)
    if res is None:
        context.log.warning("Unhandled key_operatives", value=operatives)
        return
    for item in res.operatives:
        item = dict(item)
        operative = context.make(item.pop("schema", "LegalEntity"))
        operative.id = context.make_id(
            entity.id, item["name"], item.get("country", None)
        )
        for key, value in item.items():
            operative.add(key, value)
        rel = context.make("UnknownLink")
        rel.id = context.make_id(entity.id, operative.id)
        rel.add("subject", entity.id)
        rel.add("object", operative.id)
        rel.add("role", "Key operative")
        context.emit(operative)
        context.emit(rel)


def crawl_organization(
    context: Context,
    variants: list[Item],
    org_ids: dict[str, tuple[str, str]],
    links: list[tuple[str, str]],
) -> None:
    item = variants[0]
    name_en, name_he, name_ar = drop_fallbacks(variants, "organizationName")
    entity = context.make("Organization")
    entity.id = context.make_id(name_en, name_he)
    if entity.id is None:
        context.log.warning("Organization without name", id=item["id"])
        return
    props = dict(item["properties"])
    props.pop("organizationName")
    number = props.pop("designationNumber")
    org_ids[item["id"]] = (entity.id, number)
    for linked in props.pop("linkedOrganizations") or []:
        links.append((item["id"], linked["id"]))

    entity.add("name", name_en, lang="eng")
    entity.add("name", name_he, lang="heb")
    entity.add("alias", h.multi_split(name_ar, SPLITS), lang="ara")
    apply_aliases(entity, variants, "alternativeNames")
    props.pop("alternativeNames")
    entity.add("topics", "crime.terror")
    entity.add("sourceUrl", source_url(item))

    comments, notes = parse_comments(props.pop("comments"))
    entity.add("notes", notes)
    entity.add("legalForm", props.pop("corporationType"))
    entity.add("registrationNumber", props.pop("corporationID"))
    entity.add("jurisdiction", props.pop("formationLocation"))
    apply_partial_date(
        context,
        entity,
        "incorporationDate",
        [props.pop("corporationDate")],
        comments.pop("Date of incorporation", None),
    )
    entity.add("phone", props.pop("phoneNumbers"))
    entity.add("email", props.pop("emailAddresses"))
    entity.add("website", props.pop("websites"))
    for block in pop_blocks(props, "addresses", "addressBlock"):
        apply_address(context, entity, block)

    sanction = h.make_sanction(context, entity)
    sanction.add("recordId", number)
    sanction.add("program", comments.pop("Designation type", None))
    sanction.add("sourceUrl", source_url(item))
    for block in pop_blocks(props, "justifications", "justificationBlock"):
        sanction.add("reason", html_text(block.pop("justification")))
        context.audit_data(block)
    for block in pop_blocks(props, "lastPublicationDetails", "regulationFileBlock"):
        sanction.add("publisher", block.pop("regulationFileName"))
        context.audit_data(block)
    apply_designation(context, sanction, props, comments)

    for operatives in comments.pop("Key operatives", []):
        emit_key_operatives(context, entity, operatives)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(comments)
    context.audit_data(
        props,
        ignore=[
            "isDesignated",
            "status",
            "foreignDesignator",
            "foreignDesignationDate",
        ],
    )


def crawl_operative(
    context: Context,
    variants: list[Item],
    person_ids: dict[str, str],
    links: list[tuple[str, str]],
) -> None:
    item = variants[0]
    name_en, name_he, name_ar = drop_fallbacks(variants, "fullName")
    entity = context.make("Person")
    entity.id = context.make_id(name_en, name_he, name_ar)
    if entity.id is None:
        context.log.warning("Operative without name", id=item["id"])
        return
    person_ids[item["id"]] = entity.id
    props = dict(item["properties"])
    props.pop("fullName")
    for linked in props.pop("relatedOrganizations") or []:
        links.append((item["id"], linked["id"]))

    entity.add("name", name_en, lang="eng")
    entity.add("name", name_he, lang="heb")
    entity.add("name", name_ar, lang="ara")
    apply_aliases(entity, variants, "additionalNames")
    props.pop("additionalNames")
    entity.add("topics", "crime.terror")
    entity.add("sourceUrl", source_url(item))

    comments, notes = parse_comments(props.pop("comments"))
    entity.add("notes", notes)
    entity.add("notes", comments.pop("Additional information", None))
    dobs = [b.pop("date") for b in pop_blocks(props, "datesOfBirth", "dateBlock")]
    apply_partial_date(
        context, entity, "birthDate", dobs, comments.pop("Date of birth", None)
    )
    for block in pop_blocks(props, "passportDetails", "passportDetailsBlock"):
        entity.add("nationality", h.multi_split(block.pop("nationality"), ["\n"]))
        number = block.pop("passportNumber")
        if number != "Unknown":
            entity.add("idNumber", split_ids(number))
        context.audit_data(block, ignore=["country"])
    for block in pop_blocks(
        props, "identificationDocuments", "identificationDocumentBlock"
    ):
        entity.add("idNumber", split_ids(block.pop("identificationNumber")))
        context.audit_data(block, ignore=["country", "documentType"])
    entity.add("phone", props.pop("phoneNumbers"))
    entity.add("email", props.pop("emailAddresses"))

    sanction = h.make_sanction(context, entity)
    for block in pop_blocks(props, "justifications", "justificationBlock"):
        sanction.add("program", html_text(block.pop("justification")))
        context.audit_data(block)
    sanction.add("program", props.pop("foreignDesignator"))
    sanction.add("sourceUrl", source_url(item))
    apply_designation(context, sanction, props, comments)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(comments)
    context.audit_data(
        props, ignore=["isDesignated", "status", "foreignDesignationDate"]
    )


def emit_links(
    context: Context,
    org_ids: dict[str, tuple[str, str]],
    person_ids: dict[str, str],
    links: list[tuple[str, str]],
) -> None:
    seen: set[tuple[str, str]] = set()
    for source_id, linked_id in links:
        linked = org_ids.get(linked_id)
        if linked is None:
            context.log.info("Linked organization not designated", id=linked_id)
            continue
        linked_entity_id, linked_number = linked
        source = org_ids.get(source_id)
        if source is not None:
            source_entity_id, source_number = source
            # Keep the subject/object order of the old spreadsheet-based crawler,
            # which ordered by (string) designation number, so link IDs stay stable.
            if max(source_number, linked_number) == source_number:
                subject_id, object_id = source_entity_id, linked_entity_id
            else:
                subject_id, object_id = linked_entity_id, source_entity_id
        else:
            subject_id, object_id = person_ids[source_id], linked_entity_id
        if subject_id == object_id or (subject_id, object_id) in seen:
            continue
        seen.add((subject_id, object_id))
        link = context.make("UnknownLink")
        link.id = context.make_id(subject_id, object_id)
        link.add("subject", subject_id)
        link.add("object", object_id)
        context.emit(link)


def crawl(context: Context) -> None:
    org_ids: dict[str, tuple[str, str]] = {}
    person_ids: dict[str, str] = {}
    links: list[tuple[str, str]] = []
    for variants in fetch_designated(context, "organization"):
        crawl_organization(context, variants, org_ids, links)
    for variants in fetch_designated(context, "operative"):
        crawl_operative(context, variants, person_ids, links)
    emit_links(context, org_ids, person_ids, links)
