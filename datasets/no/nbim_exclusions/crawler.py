from zavod import Context
from zavod import helpers as h


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    table = h.xpath_element(doc, ".//table")
    for row in h.parse_html_table(table, index_empty_headers=True):
        cells = h.cells_to_str(row)
        name = cells.pop("company")
        if name is None:
            continue

        entity = context.make("Company")
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("notes", cells.pop("column_1"))

        # The company name links to the press release announcing the exclusion.
        company_link = row["company"].find("./a")
        url = company_link.get("href") if company_link is not None else None
        if url is None:
            context.log.info("No link found for company", company=name)

        decision = cells.pop("decision")
        topic = context.lookup_value("decision_topic", decision)
        if topic is None:
            context.log.warning(f'Unexpected decision "{decision}"', decision=decision)
        entity.add("topics", topic)

        sanction = h.make_sanction(context, entity)
        sanction.add("description", decision)
        sanction.add("sourceUrl", url)
        sanction.add("program", cells.pop("category"))
        sanction.add("reason", cells.pop("criterion"))
        h.apply_date(sanction, "listingDate", cells.pop("publishing_date"))

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(cells)
