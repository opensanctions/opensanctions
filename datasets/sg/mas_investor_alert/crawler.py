import itertools
import json
import re
from typing import Any, NamedTuple

from pydantic import BaseModel
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract.llm import run_typed_text_prompt
from zavod.stateful.review import (
    JSONSourceValue,
    assert_all_accepted,
    review_extraction,
)

IGNORE = [
    "score",
    "approveddate_dt",
    "approveddate_s",
    # Display copies of the `_dt`/`_t` fields we read, which they agree with throughout.
    "date_s",
    "modifieddate_s",
    "last_updated_at",
    "unregulatedpersons_s",
    "alternativename_s",
    "formername_s",
    "relatedunregulatedpersons_s",
    # Always "Investor Alert List" - this API serves no other.
    "agency_custom_categories",
    # A MAS site search link, not a per-record page.
    "url",
]
NAME_SPLITTERS = [";", " / "]
# Prose naming whoever is behind the operation, resolved through a lookup. Keywords keep
# their preposition: bare "managed" also matches the 74 "Asset Management" names here.
# "affiliated with" is excluded - the source only uses it to disclaim a resemblance.
RELATIONSHIP_KEYWORDS = [
    "owned by",
    "operated by",
    "managed by",
    "controlled by",
    "run by",
    "subsidiary of",
    "division of",
    "branch of",
]
# MAS marks the party being impersonated, not the impersonator. The marked name is a
# legitimate business, and the rest of the record - the contact details, the notes, the
# listing itself - describes whoever is impersonating it.
IMPERSONATED = re.compile(r"\s*\(impersonated\)", re.IGNORECASE)
# Distinct addresses are separated by a blank line; single newlines break lines within one.
ADDRESS_SPLITTER = re.compile(r"\n\s*\n")
LLM_MODEL_VERSION = "gpt-5.4"
EXTRACT_PROMPT = """Extract structured entity data from an entry on the Monetary
Authority of Singapore's Investor Alert List.

Input: JSON with "name", "alias" and "previousName" as the source categorised them. Any
of them may hold several distinct names, a website or social media account in place of a
name, or an annotation MAS added to describe the listing.

Extract:
  name         - primary legal name(s)
  alias        - alternative or also-known-as names
  weakAlias    - short or partial names too generic to identify the entity alone
  previousName - former legal names, introduced by "formerly known as"
  abbreviation - acronyms and initialisms, usually in quotes or brackets: ("AFSCD"), ["B&A"]
  website      - URLs, bare domains and handles, even where listed as a name

Rules:
- Preserve original text exactly (no spelling corrections, no title-casing).
- Do not invent or expand names not present in the input.
- Split a value holding several names, separated by a slash, a comma or "and".
- Keep parentheses belonging to the legal name: "Quantum Securities (Singapore) Pte. Ltd"
  is one name.
- Discard a disclaimer such as "(not affiliated with "MariBank Singapore Private
  Limited")" together with the institution it names.
- Strip a platform prefix, keeping what identifies the account: 'Telegram Group: "Pictet
  Official Channel"' names the channel.
"""


class EntityData(BaseModel):
    """Websites are packed in with the names, so a reviewer needs to move values between
    them in one pass."""

    name: list[str] = []
    alias: list[str] = []
    weakAlias: list[str] = []
    previousName: list[str] = []
    abbreviation: list[str] = []
    website: list[str] = []


def apply_entity_data(
    entity: Entity, data: EntityData, origin: str | None = None
) -> None:
    for prop in EntityData.model_fields:
        for val in getattr(data, prop):
            entity.add(prop, val, origin=origin)


def emit_ownership(context: Context, entity: Entity, name: str) -> None:
    result = context.lookup("ownership", name)
    if result is not None:
        entity.add("name", result.entity_name)
        for owner_name in result.owner_name:
            owner = context.make("LegalEntity")
            owner.id = context.make_id("named", owner_name)
            owner.add("name", owner_name)
            context.emit(owner)

            own = context.make("Ownership")
            own.id = context.make_id(entity.id, owner.id)
            own.add("asset", entity)
            own.add("owner", owner)
            context.emit(own)
    else:
        context.log.warning(
            "Ownership prose in the name field needs a lookup", value=name
        )


def emit_impersonated(context: Context, entity: Entity, name: str) -> None:
    """Emit the business being impersonated, and link the listed entity to it.

    It gets no topics and no sanction: those describe the impersonator, which is the
    record's subject and keeps everything else the record says.
    """
    impersonated_name = IMPERSONATED.sub("", name)
    impersonated = context.make("LegalEntity")
    impersonated.id = context.make_id("named", impersonated_name)
    impersonated.add("name", impersonated_name)
    context.emit(impersonated)

    link = context.make("UnknownLink")
    link.id = context.make_id(entity.id, "impersonated", impersonated.id)
    link.add("subject", impersonated)
    link.add("object", entity)
    link.add("role", "Impersonated")
    context.emit(link)


def emit_relationship(
    context: Context, entity: Entity, related_ids: list[str], root_seen_ids: set[str]
) -> None:
    for rel_id in related_ids:
        # Dangling: the relation has no record of its own at the root level.
        if rel_id not in root_seen_ids:
            continue

        # The related entity is emitted from its own record.
        related_entity_id = context.make_id(rel_id)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(entity.id, "associated with", related_entity_id)
        rel.add("subject", related_entity_id)
        rel.add("object", entity.id)
        context.emit(rel)


def apply_source_names(
    context: Context,
    entity: Entity,
    unregulated_persons: list[str],
    alternative_names: list[str],
    former_names: list[str],
) -> None:
    """Map the three name fields: relationship prose through a lookup, anything else
    needing cleaning through review."""
    names: list[str] = []
    for name in h.multi_split(unregulated_persons, NAME_SPLITTERS):
        if IMPERSONATED.search(name):
            emit_impersonated(context, entity, name)
        elif any(keyword in name.lower() for keyword in RELATIONSHIP_KEYWORDS):
            emit_ownership(context, entity, name)
        else:
            names.append(name)
    aliases = h.multi_split(alternative_names, NAME_SPLITTERS)
    previous_names = h.multi_split(former_names, NAME_SPLITTERS)

    source = EntityData(name=names, alias=aliases, previousName=previous_names)
    all_names = names + aliases + previous_names
    needs_review = any(h.is_name_irregular(entity, name) for name in all_names)
    if needs_review:
        # Sorted and limited to the populated props, so that reordering at the source
        # doesn't invalidate an accepted review, and adding a property to EntityData
        # later doesn't reset every one of them.
        source_data: dict[str, Any] = {
            prop: sorted(vals) for prop, vals in source.model_dump().items() if vals
        }
        key_parts: list[str] = []
        for prop in sorted(source_data):
            key_parts.append(prop)
            key_parts.extend(source_data[prop])
        source_value = JSONSourceValue(
            key_parts=key_parts,
            label="names extraction",
            data=source_data,
        )
        result = run_typed_text_prompt(
            context=context,
            prompt=EXTRACT_PROMPT,
            string=json.dumps(source_data, ensure_ascii=False),
            response_type=EntityData,
            model=LLM_MODEL_VERSION,
        )
        review = review_extraction(
            context=context,
            source_value=source_value,
            original_extraction=result,
            origin=LLM_MODEL_VERSION,
        )
        review.link_entity(context, entity)
        if not review.accepted:
            # The crawl warns about what's still outstanding once every record is read.
            return
        if not review.extracted_data.name:
            context.log.warning("Accepted extraction has no name", entity_id=entity.id)
        apply_entity_data(entity, review.extracted_data, origin=review.origin)
    else:
        apply_entity_data(entity, source)


class CrawlItemResult(NamedTuple):
    entity: Entity
    source_id: str
    related_source_ids: list[str]


def crawl_item(context: Context, item: dict[str, Any]) -> CrawlItemResult:
    source_id = item.pop("id")

    relatedunregulatedpersonsid_s = item.pop("relatedunregulatedpersonsid_s")
    related_ids = (
        relatedunregulatedpersonsid_s.split("|")
        if relatedunregulatedpersonsid_s
        else []
    )

    entity = context.make("LegalEntity")
    entity.id = context.make_id(source_id)
    apply_source_names(
        context,
        entity,
        item.pop("unregulatedpersons_t"),
        item.pop("alternativename_t"),
        item.pop("formername_t"),
    )
    # These fields put several values on a line, separated by newlines as well as
    # punctuation. Without the newline they concatenate into one nonsense value.
    entity.add("website", h.multi_split(item.pop("website_s"), [";", ",", "\n"]))
    for phone in h.multi_split(item.pop("phonenumber_s"), ["/ ", "; ", ",", ":", "\n"]):
        # Country and Tel/Fax labels sit on their own line and carry no digits. The
        # phone cleaner rejects nothing, so they have to be dropped here.
        if any(char.isdigit() for char in phone):
            entity.add("phone", phone)
    # Older records don't use the blank-line separator consistently, so 28 values are
    # split by hand in a type.address lookup. Those have to reach it whole: the splitter
    # does divide them, and none of the resulting parts matches the lookup.
    address_s = item.pop("address_s")
    if context.lookup("type.address", address_s) is not None:
        entity.add("address", address_s)
    else:
        for address in ADDRESS_SPLITTER.split(address_s):
            entity.add("address", address)
    entity.add("notes", item.pop("notes_s"))
    entity.add("topics", ["fin", "reg.warn"])
    h.apply_date(entity, "modifiedAt", item.pop("modifieddate_dt"))
    for email in h.multi_split(item.pop("email_s"), [";", ",", ":", "/", " ", "\n"]):
        entity.add("email", email)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", item.pop("date_dt", None))

    context.audit_data(item, IGNORE)
    context.emit(entity)
    context.emit(sanction)

    return CrawlItemResult(
        entity=entity, source_id=source_id, related_source_ids=related_ids
    )


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)

    response = data["response"]
    docs = response["docs"]
    # The URL asks for a fixed row count and doesn't paginate, so a larger total means
    # the tail of the list is missing.
    num_found = response["numFound"]
    assert num_found == len(docs), (num_found, len(docs))

    crawl_item_results: list[CrawlItemResult] = []
    for item in docs:
        crawl_item_results.append(crawl_item(context, item))

    seen_ids = set(r.source_id for r in crawl_item_results)
    for result in crawl_item_results:
        emit_relationship(context, result.entity, result.related_source_ids, seen_ids)

    related_ids = set(
        itertools.chain.from_iterable(r.related_source_ids for r in crawl_item_results)
    )
    dangling_ids = related_ids - seen_ids
    if len(dangling_ids) > len(related_ids) * 0.1:
        context.log.warning(
            "Too many related IDs have no record of their own",
            dangling=len(dangling_ids),
            related=len(related_ids),
        )

    assert_all_accepted(context, raise_on_unaccepted=False)
