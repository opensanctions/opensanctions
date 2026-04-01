from lxml.etree import _Element

from zavod import Context, helpers as h


def crawl_row(context: Context, row: dict[str, _Element], sanction_date: str) -> None:
    str_row = h.cells_to_str(row)
    crd = str_row.pop("crd")
    # skip letter headers
    if crd is None:
        return
    name = str_row.pop("individual_name")

    entity = context.make("Person")
    entity.id = context.make_id(crd, name)
    entity.add("name", name)
    entity.add("idNumber", crd)
    details_url = h.xpath_string(row.get("individual_name"), ".//a/@href")
    entity.add("sourceUrl", details_url)
    entity.add("topics", "reg.action")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", sanction_date)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(str_row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_element(doc, ".//table")
    # get FINRA bar date
    sanction_date = h.xpath_string(
        doc,
        ".//div[@class='block-region-top']//p/strong[contains(text(), 'as of')]/text()",
    )
    for row in h.parse_html_table(table):
        crawl_row(context, row, sanction_date)
