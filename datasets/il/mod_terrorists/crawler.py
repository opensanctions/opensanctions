from typing import Any

import orjson
from lxml import html
from lxml.etree import _Element as Element
from rigour.mime.types import JSON
from zavod.entity import Entity
from zavod.shed.il_mod import (
    SPLITS,
    Item,
    apply_aliases,
    apply_date,
    apply_operative_details,
    apply_partial_date,
    drop_fallbacks,
    fetch_variants,
    pop_blocks,
    source_url,
)

from zavod import Context
from zavod import helpers as h


def fetch_designated(context: Context, content_type: str) -> list[list[Item]]:
    """Fetch designated items, each as a list of its [en, he, ar] culture variants."""
    designated: list[list[Item]] = []
    for item_id, variants in fetch_variants(context, content_type).items():
        # isDesignated separates designation records, active or cancelled, from
        # entities which are only named in seizure orders. It doesn't change when a
        # designation is cancelled; that's given by status and cancellationDate.
        flag = variants[0]["properties"]["isDesignated"]
        if flag is False:
            continue
        if flag is not True:
            context.log.warning("Unexpected designation flag", id=item_id, flag=flag)
            continue
        designated.append(variants)
    path = context.get_resource_path(f"{content_type}.json")
    path.write_bytes(orjson.dumps(designated))
    context.export_resource(
        path, JSON, title=f"{context.SOURCE_TITLE} ({content_type})"
    )
    return designated


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


def apply_status_topic(context: Context, entity: Entity, status: str | None) -> None:
    """Apply the topic if the designation is active"""
    if status == "Active":
        entity.add("topics", "crime.terror")
    elif status != "Cancelled":
        context.log.warning("Unexpected status", entity_id=entity.id, status=status)


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
    entity.add("sourceUrl", source_url(item))

    comments, notes = parse_comments(props.pop("comments"))
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
    sanction.add("program", props.pop("foreignDesignator"))
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

    apply_status_topic(context, entity, props.pop("status"))
    context.emit(entity)
    context.emit(sanction)
    context.audit_data(comments)
    context.audit_data(
        props,
        ignore=[
            "isDesignated",
            "foreignDesignationDate",
            "notes",
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
    entity.add("sourceUrl", source_url(item))

    comments, notes = parse_comments(props.pop("comments"))
    entity.add("notes", notes)
    entity.add("notes", comments.pop("Additional information", None))
    apply_operative_details(context, entity, props, comments.pop("Date of birth", None))

    sanction = h.make_sanction(context, entity)
    for block in pop_blocks(props, "justifications", "justificationBlock"):
        sanction.add("program", html_text(block.pop("justification")))
        context.audit_data(block)
    sanction.add("program", props.pop("foreignDesignator"))
    sanction.add("sourceUrl", source_url(item))
    apply_designation(context, sanction, props, comments)

    apply_status_topic(context, entity, props.pop("status"))
    context.emit(entity)
    context.emit(sanction)
    context.audit_data(comments)
    context.audit_data(props, ignore=["isDesignated", "foreignDesignationDate"])


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
            context.log.warning("Linked ID not found", linked_id=linked_id)
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
