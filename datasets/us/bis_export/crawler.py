from zavod import Context, helpers as h
from rigour.data.names.data import STOPPHRASES

CUSTOM_STOPPHRASES = STOPPHRASES + (",", ";", "\n")
SUFFIXES = [
    ", Inc.",
    ", LLC",
    ", Jr.",
    ", Sr.",
    ", INC.",
    ", LTD.",
    ", Ltd.",
    ", BV",
    ", et al.",
    ", S.A.",
    ", LP",
]


def crawl_entity(
    context: Context, str_row: dict, name: str, url, case_id_string, order_date
) -> None:
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, case_id_string)
    if isinstance(name, list):
        entity.add("name", name[0])
        entity.add("alias", name[1:])
    else:
        entity.add("name", name)
    entity.add("topics", "reg.warn")
    entity.add("sourceUrl", url)

    sanction = h.make_sanction(context, entity)
    sanction.add("authorityId", case_id_string)
    h.apply_date(sanction, "listingDate", order_date)  # sanction object schema date

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(str_row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_elements(doc, ".//table", expect_exactly=1)

    for row in h.parse_html_table(table[0]):
        str_row = h.cells_to_str(row)
        case_id_element = row.get("case_id")
        assert case_id_element is not None
        url = h.xpath_elements(case_id_element, ".//a")[0].get("href")

        case_name = str_row.pop("case_name")
        assert case_name is not None, case_name

        case_id_string = str_row.pop("case_id")
        order_date = str_row.pop("order_date")

        name = None
        cleaned_name = case_name
        for suffix in SUFFIXES:
            cleaned_name = cleaned_name.rstrip(suffix)
        if any(char in cleaned_name for char in CUSTOM_STOPPHRASES):
            res = context.lookup("comma_names", case_name, warn_unmatched=True)
            if res and res.entities:
                for entity_name in res.entities:
                    name = entity_name
                    crawl_entity(
                        context, str_row, name, url, case_id_string, order_date
                    )
        else:
            name = case_name
            assert name is not None
            crawl_entity(context, str_row, name, url, case_id_string, order_date)
