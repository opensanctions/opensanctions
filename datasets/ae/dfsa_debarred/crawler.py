from lxml.etree import _Element

from zavod import Context, helpers as h
from zavod.extract.zyte_api import fetch_html


ACTIONS = [
    {"action": "scrollBottom"},
]


def crawl_person(context: Context, row: _Element) -> None:
    url = row.get("href")  # link to case's PDF
    # not all rows have three values to unpack, this is a workaround to fetch text
    date, name, status = [el.tail for el in h.xpath_elements(row, ".//p/span")]

    person = context.make("Person")
    person.id = context.make_id(name, url)
    person.add("name", name)
    person.add("topics", "debarment")
    print(name)

    sanction = h.make_sanction(context, person)
    h.apply_date(sanction, "listingDate", date)
    sanction.add("sourceUrl", url)
    sanction.add("status", status)
    sanction.add("country", "ae")

    context.emit(person)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = fetch_html(
        context,
        context.data_url,
        ".//div[@class='table-content prohibited--grid']",
        actions=ACTIONS,
    )
    table = h.xpath_element(doc, ".//div[@class='table-content prohibited--grid']")

    for row in h.xpath_elements(table, ".//a"):
        crawl_person(context, row)
