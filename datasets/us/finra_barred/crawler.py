from datetime import datetime
from lxml.etree import _Element

from zavod import Context, helpers as h


def crawl_row(context: Context, row: dict[str, _Element], sanction_date: str) -> None:
    # skip letter headers
    str_row = h.cells_to_str(row)
    crd = str_row.pop("crd")
    if crd is None:
        return

    # fetch case's url
    url_el = row.get("individual_name")
    assert url_el is not None
    url = h.xpath_strings(url_el, ".//a/@href")[0]

    # collect record
    name = str_row.pop("individual_name")

    entity = context.make("Person")
    entity.id = context.make_id(crd, name)
    entity.add("name", name)
    entity.add("idNumber", crd)  # personal professional ID
    entity.add("sourceUrl", url)
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
    sanction_date = sanction_date.split("as of ")[1]

    # assert we're collecting a proper date
    try:
        datetime.strptime(sanction_date, "%B %d, %Y")
    except (ValueError, TypeError) as e:
        raise ValueError(f"Unexpected sanction_date format: {sanction_date!r}") from e

    # parse table
    for row in h.parse_html_table(table):
        crawl_row(context, row, sanction_date)
