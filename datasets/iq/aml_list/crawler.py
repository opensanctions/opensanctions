import re

from normality import latinize_text
from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.shed.trans import apply_translit_full_name
from zavod.util import LangText

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


def crawl_item(context: Context, item: dict[str, str | None]) -> None:
    entity_type = item.pop("type_en")
    decision_number = item.pop("decision_number")
    raw_name = item.pop("name")
    assert raw_name is not None, item

    # Excluded records, at the time of writing, are always "No".
    is_excluded = item.pop("is_excluded")
    if is_excluded != "No":
        assert is_excluded == "Yes", is_excluded
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
        mother_name = item.pop("mother_name")
        birth_year = item.pop("birth_year")
        # The nickname is embedded in the name and also duplicated in the
        # `alias` field, usually behind a "nicknamed" marker.
        alias_field = item.pop("alias")
        # Some records carry a court sentence in the alias field instead.
        if context.lookup("alias_not_name", alias_field) is not None:
            alias_field = None
        parts = h.multi_split(raw_name, NAME_SPLITS)
        name = parts[0]
        aliases = parts[1:] + h.multi_split(alias_field, NAME_SPLITS)

        entity = context.make("Person")
        entity.id = context.make_id(raw_name, birth_year)
        h.apply_name(entity, full=name, matronymic=mother_name, lang="ara")
        entity.add("alias", aliases, lang="ara")
        if birth_year is not None:
            h.apply_date(entity, "birthDate", birth_year)
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
    h.apply_date(sanction, "listingDate", item.pop("decision_year"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        item,
        ignore=[
            "type_ar",
            "type_ku",
            "created_at",  # when the row was inserted into the source DB
        ],
    )


def crawl(context: Context) -> None:
    # The Excel export returns the whole list in one file. Without
    # IsExcelExport, the endpoint is a paginated JSON API that caps pages at 200
    # rows and orders them unstably, so pages overlap and drop records.
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    sheet = CalamineWorkbook.from_path(path).get_sheet_by_name("Local Sanctions List")
    for item in h.parse_xlsx_sheet(context, sheet):
        crawl_item(context, item)
