from zavod import Context, helpers as h


def crawl(context: Context) -> None:
    # context.data_url with validation <== context.dataset.data.url without validation
    doc = context.fetch_html(context.data_url, cache_days=1)
    table = h.xpath_elements(doc, ".//table", expect_exactly=1)

    for row in h.parse_html_table(table[0]):
        str_row = h.cells_to_str(row)
        url = h.xpath_elements(row.get("case_id"), ".//a")[0].get("href")

        for name in h.multi_split(
            str_row.pop("case_name"), ";"
        ):  # case-level names, might contain multiple entities
            aliases = name.split("a/k/a")

            entity = context.make("LegalEntity")
            entity.id = context.make_id(
                name, str_row.get("case_id")
            )  # use raw name strings to generate IDs
            entity.add("alias", [alias.strip().replace(",", "") for alias in aliases])
            entity.add("name", entity.get("alias")[0])
            entity.add("topics", "reg.warn")  # or export.control
            entity.add("sourceUrl", context.data_url + url)

            sanction = h.make_sanction(context, entity)
            h.apply_date(
                sanction, "listingDate", str_row.get("order_date")
            )  # sanction object schema date
            context.emit(entity)
            context.emit(sanction)
            context.audit_data(str_row, ignore=["case_id", "order_date"])
