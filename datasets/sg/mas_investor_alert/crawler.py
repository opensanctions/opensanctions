import json

from rigour.mime.types import JSON
from followthemoney.types import registry

from zavod import Context, helpers as h


HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
}
IGNORE = [
    "phonenumber_s",
    "score",
    "approveddate_dt",
    "relatedunregulatedpersons_s",
]
OWNERSHIP_KEYWORDS = ["owned ", "managed ", "operates ", "operated "]
WEBSITE_KEYWORDS = [".com", ".net", ".org", "https:"]


def emit_ownership(context, entity, owner_name, name):
    result = context.lookup("ownership", name)
    if result is not None:
        entity.add("name", result.entity_name)
        owner_names = result.owner_name
        # Mostly we have only one owner, but sometimes we have multiple
        for owner_name in owner_names:
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
        context.log.warning(f'Name "{name}" needs to be remapped', value=name)


def emit_relationship(context, entity, relatedunregulatedpersonsid_s):
    related_ids = relatedunregulatedpersonsid_s.split("|")
    for rel_id in related_ids:
        # No need to emit, since we have them already
        related = context.make("LegalEntity")
        related.id = context.make_id(rel_id)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(entity.id, "associated with", related.id)
        rel.add("subject", related.id)
        rel.add("object", entity.id)
        context.emit(rel)


def add_lookup_items(context, entity, name):
    res = context.lookup("names", name)
    if res:
        # This lookup may return either a 'name', a 'website', or both.
        for lookup_item in res.items:
            entity.add(lookup_item["prop"], lookup_item["value"])
    else:
        context.log.warning(f'Name "{name}" needs to be remapped', value=name)


def crawl_item(context: Context, item: dict):
    id = item.pop("id")
    relatedunregulatedpersonsid_s = item.pop("relatedunregulatedpersonsid_s")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(id)
    names = h.multi_split(item.pop("unregulatedpersons_t"), [";", " / "])
    for name in names:
        if any(keyword in name for keyword in WEBSITE_KEYWORDS):
            add_lookup_items(context, entity, name)
        elif any(keyword in name for keyword in OWNERSHIP_KEYWORDS):
            emit_ownership(context, entity, name, name)
        else:
            entity.add("name", name)
    entity.add("alias", h.multi_split(item.pop("alternativename_t"), [";"]))
    entity.add("previousName", item.pop("formername_t"))
    entity.add("website", h.multi_split(item.pop("website_s"), [";"]))
    entity.add("address", item.pop("address_s"))
    entity.add("notes", item.pop("notes_s"))
    entity.add("topics", ["fin", "reg.warn"])
    h.apply_date(entity, "modifiedAt", item.pop("modifieddate_dt"))
    for email in h.multi_split(item.pop("email_s"), [";", " and "]):
        email_clean = registry.email.clean(email)
        if email_clean is not None:
            entity.add("email", email)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", item.pop("date_dt", None))

    if relatedunregulatedpersonsid_s:
        emit_relationship(context, entity, relatedunregulatedpersonsid_s)

    context.audit_data(item, IGNORE)
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource(
        "source.json",
        context.data_url,
        headers=HEADERS,
    )
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    for item in data.get("response").get("docs"):
        crawl_item(context, item)
