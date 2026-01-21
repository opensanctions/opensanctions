from zavod import Context, helpers as h


def split_name_country(context: Context, original_name: str) -> tuple[str, str | None]:
    res = context.lookup("names", original_name)
    if res:
        return res.name, res.country
    return original_name, None


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=3, absolute_links=True)
    table = h.xpath_element(doc, "//table[@class='mb-4 overflow-auto block']")

    for row in h.parse_html_table(table, slugify_headers=True):
        cell = row.get("case_id")
        assert cell is not None, cell
        url = h.xpath_string(cell, ".//a/@href")

        str_row = h.cells_to_str(row)
        case_name = str_row.pop("case_name")
        assert case_name, case_name

        clean_name, country = split_name_country(context, case_name)
        if "(" in clean_name:
            context.log.warning(
                "Name looks like it should be split, please add to names lookup.",
                name=case_name,
            )

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
