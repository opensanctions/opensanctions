import json
import re
from typing import Any

from normality import latinize_text
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h
from zavod.shed.trans import apply_translit_full_name
from zavod.util import LangText

# The local sanctions list is served by the platform's JSON API. Its paginated
# ordering is unstable — successive pages overlap and drop records — so we fetch
# the whole list in a single request per sort order.
#
# !!! HACK !!!
# The API currently returns HTTP 500 for any request reaching past roughly the
# 5040th row (reported upstream), so we fetch the first PAGE_SIZE rows sorted by
# date ascending and descending, and take the union. That covers the list as
# long as it holds at most 2 * PAGE_SIZE rows.
# Smaller pages doesn't resolve it.
# TODO: We've informed them of this bug in their API and haven't heard back.
# There's a new record so let's hack it for now but revert to a single forward
# crawl once they've fixed it.
PAGE_SIZE = 5_000

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
    entity_type = item.pop("typeEn")
    decision_number = item.pop("decisionNumber")
    raw_name = item.pop("name")

    # Excluded records, at the time of writing, are always False.
    is_excluded = item.pop("isExcluded")
    if is_excluded:
        return

    if entity_type == "Entities":
        name = ENTITY_NAME_REASON.sub("", raw_name).strip()
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, decision_number)
        entity.add("name", name, lang="ara")
        sector = ENTITY_NAME_REASON.search(raw_name)
        if sector is not None:
            entity.add("sector", sector.group().strip(), lang="ara")
        if name != latinize_text(name):
            apply_translit_full_name(context, entity, LangText(name, "ara"))
    elif entity_type == "Individuals":
        mother_name = item.pop("motherName")
        birth_year = item.pop("birthYear")
        # The nickname is embedded in the name and also duplicated in the
        # `alias` field, usually behind a "nicknamed" marker.
        alias_field = item.pop("alias")
        parts = h.multi_split(raw_name, NAME_SPLITS)
        name = parts[0]
        aliases = parts[1:] + h.multi_split(alias_field, NAME_SPLITS)

        entity = context.make("Person")
        entity.id = context.make_id(raw_name, birth_year)
        h.apply_name(entity, full=name, matronymic=mother_name, lang="ara")
        entity.add("alias", aliases, lang="ara")
        if birth_year is not None:
            h.apply_date(entity, "birthDate", str(birth_year))
        if name != latinize_text(name):
            apply_translit_full_name(context, entity, LangText(name, "ara"))
        for alias in aliases:
            if alias != latinize_text(alias):
                apply_translit_full_name(
                    context, entity, LangText(alias, "ara"), alias=True
                )
    else:
        context.log.warning("Unknown entity type", type=entity_type, name=raw_name)
        return

    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("recordId", decision_number)
    h.apply_date(sanction, "listingDate", item.pop("decisionYear"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        item,
        ignore=[
            "id",
            "createdAt",  # when the row was inserted into the source DB
            "type",
            "typeAr",
            "typeKu",
            "excludeAttachment",
        ],
    )


def crawl(context: Context) -> None:
    items: dict[str, dict[str, Any]] = {}
    row_counts: set[int] = set()
    for order in ("asc", "desc"):
        is_asc = "true" if order == "asc" else "false"
        path = context.fetch_resource(
            f"source_{order}.json",
            f"{context.data_url}?pageSize={PAGE_SIZE}&SortByDate=true&IsAsc={is_asc}",
        )
        context.export_resource(path, JSON, title=f"{context.SOURCE_TITLE} ({order})")
        with open(path) as fh:
            data = json.load(fh)["data"]
        row_counts.add(data["rowCount"])
        for item in data["data"]:
            items[item["id"]] = item

    assert len(row_counts) == 1, row_counts
    row_count = row_counts.pop()

    # !!! HACK see note at PAGE_SIZE
    if row_count > 2 * PAGE_SIZE:
        context.log.warning(
            "List outgrew what the ascending/descending union can cover",
            row_count=row_count,
            max_rows=2 * PAGE_SIZE,
        )
    # !!! HACK see note at PAGE_SIZE
    if len(items) != row_count:
        context.log.warning(
            "Union of sort orders doesn't cover the whole list",
            fetched=len(items),
            row_count=row_count,
        )
    for item in items.values():
        crawl_item(context, item)
