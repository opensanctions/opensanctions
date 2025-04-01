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
        id = item.pop("id")
        relatedunregulatedpersonsid_s = item.pop("relatedunregulatedpersonsid_s")

        entity = context.make("LegalEntity")
        entity.id = context.make_id(id)
        names = h.multi_split(item.pop("unregulatedpersons_t"), [";", " / "])
        for name in names:
            if ".com" in name:
                res = context.lookup("names", name)
                if res:
                    for lookup_item in res.items:
                        entity.add(lookup_item["prop"], lookup_item["value"])
                else:
                    context.log.warning(
                        f'Name "{name}" needs to be remapped', value=name
                    )
            elif "owned by" in name:
                result = context.lookup("ownership", name)
                if result is not None:
                    entity.add("name", result.entity_name)
                    owner_name = result.owner_name
                    owner = context.make("LegalEntity")
                    owner.id = context.make_id("named", owner_name)
                    owner.add("name", owner_name)
                    context.emit(owner)

                    link = context.make("Ownership")
                    link.id = context.make_id(entity.id, owner.id)
                    link.add("asset", entity)
                    link.add("owner", owner)
                    context.emit(link)
                else:
                    context.log.warning(
                        f'Name "{name}" needs to be remapped', value=name
                    )
            else:
                entity.add("name", name)
        entity.add("alias", h.multi_split(item.pop("alternativename_t"), [";"]))
        entity.add("previousName", item.pop("formername_t"))
        entity.add("website", h.multi_split(item.pop("website_s"), [";"]))
        for email in h.multi_split(item.pop("email_s"), [";", " and "]):
            email_clean = registry.email.clean(email)
            if email_clean is not None:
                entity.add("email", email)
        entity.add("address", item.pop("address_s"))
        entity.add("notes", item.pop("notes_s"))
        entity.add("topics", ["fin", "reg.warn"])
        h.apply_date(entity, "modifiedAt", item.pop("modifieddate_dt"))

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "listingDate", item.pop("date_dt", None))

        if relatedunregulatedpersonsid_s:
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

        context.audit_data(item, IGNORE)
        context.emit(entity)
        context.emit(sanction)
