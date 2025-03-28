import json

from rigour.mime.types import JSON
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
        # relatedunregulatedpersons_s = item.pop("relatedunregulatedpersons_s")

        entity = context.make("LegalEntity")
        entity.id = context.make_id(id)
        entity.add("name", item.pop("unregulatedpersons_t"))
        entity.add("alias", item.pop("alternativename_t"))
        entity.add("previousName", item.pop("formername_t"))
        entity.add("website", item.pop("website_s"))
        entity.add("email", h.multi_split(item.pop("email_s"), [";", " and "]))
        entity.add("address", item.pop("address_s"))
        entity.add("notes", item.pop("notes_s"))
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
