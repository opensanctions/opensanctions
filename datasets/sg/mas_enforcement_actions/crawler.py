from zavod import Context, helpers as h


# def crawl_enforcement_action(context: Context, url: str, date: str, action_type: str):
#     article = context.fetch_html(url, cache_days=7)
#     article.make_links_absolute(context.data_url)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    table = doc.xpath("//table")
    assert len(table) == 1, "Expected exactly one table in the document"
    for row in h.parse_html_table(table[0]):
        links = h.links_to_dict(row.pop("title"))
        str_row = h.cells_to_str(row)
        date = str_row.pop("issue_date")
        # entities = str_row.pop("person_company")
        action_type = str_row.pop("action_type")
        context.audit_data(str_row)
        url = next(iter(links.values()))
        # crawl_enforcement_action(context, url, date, action_type)
