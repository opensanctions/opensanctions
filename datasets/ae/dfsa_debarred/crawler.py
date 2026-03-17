from lxml.etree import _Element
from lxml.html import document_fromstring

from zavod import Context, helpers as h
from zavod.extract.zyte_api import fetch_text


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
    h.apply_date(sanction, "startDate", date)
    sanction.add("sourceUrl", url)
    sanction.add("country", "ae")
    sanction.add("status", status)

    # add endDate if the status is expired & don't add debarment topic
    if "past" in status.lower():
        date_end = status.split("(")[1]
        date_end = date_end.strip(")")
        date_end = date_end.replace("expired ", "")
        h.apply_date(sanction, "endDate", date_end)
    elif "ongoing" in status.lower():
        person.add("topics", "debarment")
    elif status.strip():
        context.log.warning(f"Unexpected case status, status={status}")

    context.emit(person)
    context.emit(sanction)


def crawl(context: Context) -> None:
    page_num = 1

    # using Zyte to bypass the 403 status response
    # paginate over the AJAX endpoint until an empty reponse
    while True:
        html_text = fetch_text(
            context,
            f"{context.data_url}?page={page_num}&status=&keywords=&isAjax=true",
            expected_media_type="text/html",
            expected_charset="utf-8",
        )
        # pagination gives empty response after last page
        if html_text[3] == "":
            break

        doc = document_fromstring(html_text[3])
        for row in h.xpath_elements(doc, ".//a[@class='table-row']"):
            crawl_person(context, row)

        page_num += 1
