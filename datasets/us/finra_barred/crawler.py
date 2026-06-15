from zavod import Context, helpers as h


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_element(doc, ".//table")
    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)
        crd = str_row.pop("crd")
        # skip letter headers
        if crd is None:
            continue
        name = str_row.pop("individual_name")

        entity = context.make("Person")
        entity.id = context.make_id(crd, name)
        entity.add("name", name)
        entity.add("idNumber", crd)
        url_el = row.get("individual_name")
        assert url_el is not None
        entity.add("sourceUrl", h.xpath_string(url_el, ".//a/@href"))
        entity.add("topics", "reg.action")
        entity.add("country", "us")

        context.emit(entity)
        context.audit_data(str_row)
