import re
from typing import Any

from normality import latinize_text

from zavod import Context
from zavod import helpers as h
from zavod.shed.trans import apply_translit_full_name, ENGLISH

# The local sanctions list is served by the platform's JSON API. Its paginated
# ordering is unstable — successive pages overlap and drop records — so we fetch
# the whole list in a single request with a page size well above the row count
# and assert it wasn't truncated.
API_URL = "https://api.aml-iq.com/api/localsanctionslist/getall/1"
PAGE_SIZE = 100_000

# The `type` field distinguishes natural persons from organisations.
TYPE_INDIVIDUAL = 0
TYPE_ENTITY = 1

TRANSLIT_OUTPUT = [ENGLISH]

# Nicknames are appended to the primary name, either introduced by a
# "nicknamed" marker or wrapped in parentheses/quotes. Splitting on these
# separators yields the clean name in the first segment and the nickname(s)
# in the rest.
NAME_SPLITS = [
    "المكنى",  # nicknamed
    "الملقبة",  # nicknamed (feminine)
    "الملقب",  # nicknamed
    "(",
    ")",
    '"',
]

# Currency-exchange companies encode their licensed activity as a name suffix.
# We strip it from the name and record it as the sector.
# "to mediate in the sale and purchase of foreign currencies"
ENTITY_NAME_REASON = re.compile(r"\s*للتوسط ببيع وشراء العملات الاجنبية$")


def crawl_item(context: Context, item: dict[str, Any]) -> None:
    entity_type = item.pop("type")
    decision_number = item.pop("decisionNumber")
    decision_year = item.pop("decisionYear")
    raw_name = item.pop("name")

    # Excluded (delisted) records carry a removal attachment. We do not have a
    # confirmed example yet, so surface them for review rather than guessing
    # how to represent a delisting.
    is_excluded = item.pop("isExcluded")
    exclude_attachment = item.pop("excludeAttachment")
    if is_excluded:
        context.log.warning(
            "Skipping excluded record; representation not yet defined",
            name=raw_name,
            decision_number=decision_number,
            exclude_attachment=exclude_attachment,
        )
        return

    if entity_type == TYPE_ENTITY:
        name = ENTITY_NAME_REASON.sub("", raw_name).strip()
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, decision_number)
        entity.add("name", name, lang="ara")
        sector = ENTITY_NAME_REASON.search(raw_name)
        if sector is not None:
            entity.add("sector", sector.group().strip(), lang="ara")
        if name != latinize_text(name):
            apply_translit_full_name(context, entity, "ara", name, TRANSLIT_OUTPUT)
    elif entity_type == TYPE_INDIVIDUAL:
        mother_name = item.pop("motherName")
        birth_year = item.pop("birthYear")
        # The nickname is embedded in the name and also duplicated in the
        # `alias` field, usually behind a "nicknamed" marker.
        alias_field = item.pop("alias")
        parts = h.multi_split(raw_name, NAME_SPLITS)
        name = parts[0]
        aliases = parts[1:] + h.multi_split(alias_field, NAME_SPLITS)

        entity = context.make("Person")
        # ID scheme kept identical to the pre-migration crawler (name + birth
        # year) so published Person IDs stay stable across the source move.
        # People listed under several decisions collapse into one entity, whose
        # sanction accumulates each decision's recordId/listingDate.
        entity.id = context.make_id(raw_name, birth_year)
        h.apply_name(entity, full=name, matronymic=mother_name, lang="ara")
        entity.add("alias", aliases, lang="ara")
        if birth_year is not None:
            h.apply_date(entity, "birthDate", str(birth_year))
        if name != latinize_text(name):
            apply_translit_full_name(context, entity, "ara", name, TRANSLIT_OUTPUT)
        for alias in aliases:
            if alias != latinize_text(alias):
                apply_translit_full_name(
                    context, entity, "ara", alias, TRANSLIT_OUTPUT, alias=True
                )
    else:
        context.log.warning("Unknown entity type", type=entity_type, name=raw_name)
        return

    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("recordId", decision_number)
    if decision_year is not None:
        h.apply_date(sanction, "listingDate", str(decision_year))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(item, ["id", "createdAt", "typeAr", "typeEn", "typeKu"])


def crawl(context: Context) -> None:
    payload = context.fetch_json(
        API_URL,
        params={"pageSize": PAGE_SIZE},
        cache_days=1,
    )
    if payload.get("error"):
        raise RuntimeError(f"API error: {payload.get('message')}")
    data = payload["data"]
    items = data["data"]
    # The single request must cover the whole list; a second page would mean the
    # list outgrew PAGE_SIZE and we'd hit the unstable pagination.
    assert data["pageCount"] == 1, data["pageCount"]
    assert len(items) == data["rowCount"], (len(items), data["rowCount"])
    for item in items:
        crawl_item(context, item)
