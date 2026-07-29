import itertools
import json
import re
from typing import Any, NamedTuple

from followthemoney.types import registry
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

IGNORE = [
    "score",
    "approveddate_dt",
    "approveddate_s",
    # The API renders every date and multi-valued field a second time as a display
    # string. We read the `_dt`/`_t` originals, which agree with these throughout.
    "date_s",
    "modifieddate_s",
    "last_updated_at",
    "unregulatedpersons_s",
    "alternativename_s",
    "formername_s",
    "relatedunregulatedpersons_s",
    # Constant "Investor Alert List" - this API only serves that list.
    "agency_custom_categories",
    # A link to a MAS site search for the entity name, not a per-record page.
    "url",
]
OWNERSHIP_KEYWORDS = ["owned ", "managed ", "operates ", "operated "]
WEBSITE_KEYWORDS = [".com", ".net", ".org", "https:", "http:", "www.", ".sg", ".co"]
NAME_SPLITTERS = [";", " / "]
# Distinct addresses are separated by a blank line. Single newlines are line breaks
# inside one address, which the address type cleaner joins up with commas.
ADDRESS_SPLITTER = re.compile(r"\n\s*\n")


def emit_ownership(context: Context, entity: Entity, name: str) -> None:
    result = context.lookup("ownership", name)
    if result is not None:
        entity.add("name", result.entity_name)
        # Mostly we have only one owner, but sometimes we have multiple
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
        context.log.warning("Name needs to be remapped", value=name)


def emit_relationship(
    context: Context, entity: Entity, related_ids: list[str], root_seen_ids: set[str]
) -> None:
    for rel_id in related_ids:
        if rel_id not in root_seen_ids:
            # The relations described here should have a peer at the root level, otherwise they are dangling.
            # Skip those dangling ones here.
            continue

        # No need to emit related entities since they're already included
        # at the root level of the response
        related_entity_id = context.make_id(rel_id)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(entity.id, "associated with", related_entity_id)
        rel.add("subject", related_entity_id)
        rel.add("object", entity.id)
        context.emit(rel)


def add_lookup_items(context: Context, entity: Entity, name: str) -> None:
    res = context.lookup("names", name)
    if res is not None:
        # This lookup may return either a 'name', a 'website', or both.
        for lookup_item in res.items:
            entity.add(lookup_item["prop"], lookup_item["value"])
    else:
        context.log.warning("Name needs to be remapped", value=name)


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
    names = h.multi_split(item.pop("unregulatedpersons_t"), NAME_SPLITTERS)
    for name in names:
        if any(keyword in name for keyword in WEBSITE_KEYWORDS):
            add_lookup_items(context, entity, name)
        elif any(keyword in name for keyword in OWNERSHIP_KEYWORDS):
            emit_ownership(context, entity, name)
        else:
            entity.add("name", name)
    entity.add("alias", h.multi_split(item.pop("alternativename_t"), NAME_SPLITTERS))
    entity.add("previousName", item.pop("formername_t"))
    # Each of these fields lists several values to a line, separated by newlines on
    # top of punctuation. Without splitting on the newline, distinct values get
    # concatenated into one nonsense value.
    entity.add("website", h.multi_split(item.pop("website_s"), [";", ",", "\n"]))
    for phone in h.multi_split(item.pop("phonenumber_s"), ["/ ", "; ", ",", ":", "\n"]):
        # Numbers are labelled with a country or with Tel/Fax on their own line.
        # Unlike a phone number, a label carries no digits. Phone values are never
        # rejected by the cleaner, so these have to be dropped here.
        if any(char.isdigit() for char in phone):
            entity.add("phone", phone)
    # Several addresses for one entity are separated by a blank line. Older records
    # aren't consistent about that, so where a type.address lookup splits the whole
    # value by hand, leave it to do the job.
    address_s = item.pop("address_s")
    if context.lookup("type.address", address_s) is not None:
        entity.add("address", address_s)
    else:
        for address in ADDRESS_SPLITTER.split(address_s):
            entity.add("address", address)
    entity.add("notes", item.pop("notes_s"))
    entity.add("topics", ["fin", "reg.warn"])
    h.apply_date(entity, "modifiedAt", item.pop("modifieddate_dt"))
    # None of these separators can occur inside an email address, and the field also
    # carries country labels ("Singapore: a@b.com") and space-separated lists.
    for email in h.multi_split(item.pop("email_s"), [";", ",", ":", "/", " ", "\n"]):
        # Splitting that aggressively leaves fragments of the surrounding free text,
        # so only keep the ones that are actually email addresses.
        email_clean = registry.email.clean(email)
        if email_clean is not None:
            entity.add("email", email_clean)

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
    # The data URL asks for a fixed number of rows and the crawler doesn't paginate,
    # so a total larger than what came back means the tail of the list is missing.
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
