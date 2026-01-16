from zavod import Context, helpers as h


def split_name_country(context: Context, original_name: str):
    res = context.lookup("names", original_name, warn_unmatched=True)
    if res:
        return res.name, res.country
    return original_name, ""


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=3, absolute_links=True)
    table = h.xpath_elements(
        doc, "//table[@class='mb-4 overflow-auto block']", expect_exactly=1
    )[0]
    for row in h.parse_html_table(table, slugify_headers=True):
        cell = row.get("case_id")
        assert cell is not None, cell
        url = h.xpath_elements(cell, ".//a")[0].get("href")
        str_row = h.cells_to_str(row)
        case_name = str_row.pop("case_name")
        assert case_name, case_name
        if "(" in case_name:
            clean_name, country = split_name_country(context, case_name)
        else:
            clean_name = case_name
            country = ""

        entity = context.make("LegalEntity")
        entity.id = context.make_id(str_row.pop("case_id"), case_name)
        entity.add("country", country)
        entity.add("name", clean_name)
        entity.add("sourceUrl", url)
        entity.add("country", country)
        entity.add("topics", "export.control.linked")

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "listingDate", str_row.pop("order_date"))

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(str_row)
