from zavod import Context, helpers as h


def crawl(context: Context) -> None:
    # context.data_url with validation <== context.dataset.data.url without validation
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_elements(doc, ".//table", expect_exactly=1)

    for row in h.parse_html_table(table[0]):
        str_row = h.cells_to_str(row)
        case_id = str_row.get("case_id")
        assert case_id is not None
        url = h.xpath_elements(row.get("case_id"), ".//a")[0].get("href")

        case_name = str_row.pop("case_name")
        for name in h.multi_split(
            case_name, ";"
        ):  # case-level names, might contain multiple entities
            aliases = name.split("a/k/a")

            entity = context.make("LegalEntity")
            entity.id = context.make_id(
                name, case_id
            )  # use raw name strings to generate IDs
            entity.add("alias", [alias.strip().replace(",", "") for alias in aliases])
            entity.add("name", entity.get("alias")[0])
            entity.add("topics", "reg.warn")  # or export.control
            entity.add("sourceUrl", url)

            sanction = h.make_sanction(context, entity)
            sanction.add("authorityId", case_id)
            h.apply_date(
                sanction, "listingDate", str_row.get("order_date")
            )  # sanction object schema date

            context.emit(entity)
            context.emit(sanction)
            context.audit_data(str_row, ignore=["order_date", "case_id"])
