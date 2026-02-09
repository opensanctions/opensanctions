from rigour.data.names.data import STOPPHRASES

from zavod import Context, helpers as h

CUSTOM_STOPPHRASES = [phrase.casefold() for phrase in STOPPHRASES + (",", ";", "\n")]
SUFFIXES = [
    ", III.",
    ", Inc.",
    ", L.L.C.",
    ", LLC.",
    ", LLC",
    " LLC",
    " LLC.",
    ", Jr.",
    ", SR.",
    ", Sr.",
    ", INC.",
    ", Inc",
    ", LTD.",
    " LTD.",
    ", Ltd.",
    " Ltd.",
    ", BV",
    ", et al.",
    ", S.A.",
    " Co., SAL",
    " Corp.",
    ", SAL",
    ", S.L.",
    ", LP",
    " F.Z.E.",
]


def crawl_entity(
    context: Context,
    name: str | list[str],
    url: str,
    case_id_string: str,
    order_date: str,
) -> None:
    # Lookup returns [primary_name, alias1, alias2, ...] or single string
    primary_name = name[0] if isinstance(name, list) else name
    aliases = name[1:] if isinstance(name, list) else []

    entity = context.make("LegalEntity")
    entity.id = context.make_id(primary_name, case_id_string)
    entity.add("name", primary_name)
    for alias in aliases:
        entity.add("alias", alias)

    entity.add("topics", "reg.warn")
    entity.add("sourceUrl", url)

    sanction = h.make_sanction(context, entity)
    sanction.add("authorityId", case_id_string)
    h.apply_date(sanction, "listingDate", order_date)

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_element(doc, ".//table")

    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)
        case_id_element = row.get("case_id")
        assert case_id_element is not None
        url = h.xpath_string(case_id_element, ".//a/@href")

        case_name = str_row.pop("case_name")
        assert case_name is not None, case_name

        case_id_string = str_row.pop("case_id")
        order_date = str_row.pop("order_date")
        assert case_id_string and order_date

        cleaned_name = case_name
        # Remove common suffixes (Inc., LLC, etc.) to check for delimiters
        for suffix in SUFFIXES:
            cleaned_name = cleaned_name.removesuffix(suffix)
        # Check if the name contains delimiters indicating multiple entities (commas, semicolons, "and", etc.)
        if any(phrase in cleaned_name for phrase in CUSTOM_STOPPHRASES):
            res = context.lookup("comma_names", case_name, warn_unmatched=True)
            if res and res.entities:
                # When parsing comma-separated strings with multiple entities, the lookup
                # returns each entity as either a string or a list [primary_name, ...aliases]
                # if that entity has multiple name variants (e.g., "Company A a/k/a Company A2")
                for entity_name in res.entities:
                    crawl_entity(context, entity_name, url, case_id_string, order_date)
        else:
            # Simple case: single entity name with no delimiters
            crawl_entity(context, case_name, url, case_id_string, order_date)

        context.audit_data(str_row)
