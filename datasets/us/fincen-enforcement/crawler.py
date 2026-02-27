from lxml.etree import _Element
from zavod import Context, helpers as h
from rigour.names.split_phrases import contains_split_phrase


def crawl_row(context: Context, row: dict[str, _Element]) -> None:
    # fetch case's url
    url_el = row.get("enforcement_action")
    assert url_el is not None
    url = h.xpath_strings(url_el, ".//a/@href")

    # process row
    str_row = h.cells_to_str(row)
    case_name = str_row.pop("enforcement_action")

    assert case_name is not None
    if "in the matter of" in case_name.lower():
        case_name = case_name.split("In the Matter of")[1].strip()

    entity = context.make("LegalEntity")
    entity.id = context.make_id(case_name, str_row.pop("matter_number"))

    # custom lookups to split entities and aliases
    if contains_split_phrase(case_name) or " and " in case_name:
        res = context.lookup("comma_names", case_name, warn_unmatched=True)
        if res and res.entities:
            for entity_name in res.entities:
                primary_name = entity_name[0]
                entity.add("name", primary_name)
                aliases = entity_name[1:] if len(entity_name) > 1 else []
                for alias in aliases:
                    entity.add("alias", alias)
        else:
            print(case_name)
            entity.add("name", case_name)

    entity.add("sourceUrl", url)
    entity.add("sector", str_row.pop("financial_institution"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", str_row.pop("date_sort_ascending"))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(str_row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_element(doc, ".//table")

    for row in h.parse_html_table(table):
        crawl_row(context, row)
