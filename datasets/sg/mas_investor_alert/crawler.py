import json

from rigour.mime.types import JSON
from followthemoney.types import registry

from zavod import Context, helpers as h


IGNORE = [
    "score",
    "approveddate_dt",
    "relatedunregulatedpersons_s",
]
OWNERSHIP_KEYWORDS = ["owned ", "managed ", "operates ", "operated "]
WEBSITE_KEYWORDS = [".com", ".net", ".org", "https:", "http:", "www.", ".sq", ".co"]
NON_EXISTENT_RELATIONS = {"5685", "4891"}

all_entities = set()
all_related = set()


def check_num_found(context, data):
    num_found = data.get("response").get("numFound")
    if num_found is None:
        context.log.warn("Response doesn't contain numFound field")
    else:
        if num_found > 999:
            context.log.warn(
                "More entities than currently covered", source_url=context.data_url
            )


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
        if rel_id in NON_EXISTENT_RELATIONS:
            # Skip non-existent relations
            continue

        # Add the ID to the set of all related entities
        global all_related
        all_related.add(rel_id)

        # No need to emit related entities since they're already included
        # at the root level of the response
        related_entity_id = context.make_id(rel_id)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(entity.id, "associated with", related_entity_id)
        rel.add("subject", related_entity_id)
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
    # Add the ID to the set of all entities
    global all_entities
    all_entities.add(id)

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
    entity.add("phone", h.multi_split(item.pop("phonenumber_s"), ["/ ", "; "]))
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
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    check_num_found(context, data)
    for item in data.get("response").get("docs"):
        crawl_item(context, item)

    # Check for unknown related entity IDs
    missing_related_ids = all_related - all_entities - NON_EXISTENT_RELATIONS
    if missing_related_ids:
        context.log.warning(
            f"Found {len(missing_related_ids)} new unknown related entity IDs",
            missing_ids=list(missing_related_ids),
        )
