import re
from typing import Any
from collections.abc import Generator
from rigour.ids import LEI
from lxml import html

from zavod import Context, helpers as h


def get_json(
    context: Context, batch_size: int
) -> Generator[dict[str, Any], None, None]:
    params: dict[str, Any] = {
        "q": "*",
        "rows": batch_size,
        "start": 0,
        "wt": "json",
    }
    cont = True
    while cont:
        resp = context.fetch_json(context.data_url, params=params)
        yield from resp["response"]["docs"]
        params["start"] += batch_size
        cont = resp["response"]["numFound"] >= params["start"]


def parse_name(name_markup: str) -> str | None:
    """
    Remove markup from name if present.
    """
    if name_markup is None:
        return None
    return h.element_text(html.fromstring(name_markup))


def crawl(context: Context) -> None:
    for row in get_json(context, 1000):
        id_hash = context.make_id(
            row.pop("sn_entityEsmaID", None),
            row.get("sn_entityLEI"),
            row.get("sn_entityName"),
            row.get("sn_otherEntityName"),
            row.get("sn_otherEntityLEI"),
        )
        if id_hash is None or row.get("sn_sanctionEsmaID") is None:
            # This is a very common case that we're currently ignoring where
            # the entity are persons mentioned in a plain text or on some other URL.
            context.log.info("Skipping row without entity or sanction ID", row=row)
            continue
        entity = context.make("Company")
        lei = row.pop("sn_entityLEI", None)
        other_lei = row.pop("sn_otherEntityLEI", None)
        if lei is not None and LEI.is_valid(lei):
            entity.id = f"lei-{lei}"
            entity.add("leiCode", lei)
        elif other_lei is not None and LEI.is_valid(other_lei):
            entity.id = f"lei-{other_lei}"
            entity.add("leiCode", other_lei)
        else:
            entity.id = id_hash
        entity.add("name", parse_name(row.pop("sn_entityName", None)))
        entity.add("name", parse_name(row.pop("sn_otherEntityName", None)))
        entity.add("topics", "reg.warn")

        sanction = h.make_sanction(context, entity)
        sanction.add("program", row.pop("sn_sanctionLegalFrameworkName", None))
        sanction.add("startDate", row.pop("sn_date", None))
        sanction.add("endDate", row.pop("sn_expirationDate", None))
        # Sometimes, the reason has some HTML tags in it, like so
        # <a href='http://www.fi.se/Tillsyn/Sanktioner/Finansiella'>http://www.fi.se/Tillsyn/Sanktioner/Finansiella</a>-foretag/Listan/Anmarkning-och-straffavgift-for-Larsson--Partners/
        # but the relevant URL is always also included as text outside the tags.
        reason_raw = row.pop("sn_text", "")
        reason_cleaned = re.sub(r"<[^>]+>", "", reason_raw)
        sanction.add("reason", reason_cleaned)
        sanction.add("provisions", row.pop("sn_natureFullName", None))
        sanction.set("authority", row.pop("sn_ncaCodeFullName", None))
        sanction.set("country", row.pop("sn_countryName", None))
        context.emit(entity)
        context.emit(sanction)
