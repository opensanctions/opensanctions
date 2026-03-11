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
    assert name is not None and status is not None

    # skip anonymized entries
    if name.lower() == "an individual":
        return

    person = context.make("Person")
    person.id = context.make_id(name, url)
    person.add("name", name)

    sanction = h.make_sanction(context, person)
    h.apply_date(sanction, "listingDate", date)
    sanction.add("sourceUrl", url)
    sanction.add("country", "ae")
    sanction.add("status", status)

    # add endDate if the status is expired & don't add debarment topic
    if "past" in status.lower():
        date_end = status.split("(")[1]
        date_end = date_end.strip(")")
        date_end = date_end.replace("expired ", "")
        h.apply_date(sanction, "endDate", date_end)
    else:
        person.add("topics", "debarment")

    context.emit(person)
    context.emit(sanction)


def crawl(context: Context) -> None:
    xpath_table = ".//div[@class='table-content prohibited--grid']"

    # using Zyte API to bypass the 403 error
    doc = fetch_html(
        context,
        context.data_url,
        xpath_table,
        actions=ACTIONS,
    )
    table = h.xpath_element(doc, xpath_table)

    for row in h.xpath_elements(table, ".//a"):
        crawl_person(context, row)
