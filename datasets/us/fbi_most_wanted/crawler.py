import re
import math
from itertools import count
from typing import Optional
from lxml import etree

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

FORMATS = (
    "%B %d, %Y",
    "%d/%m/%Y",
)
FBI_URL = "https://www.fbi.gov/wanted/%s/@@castle.cms.querylisting/%s?page=%s"
IGNORE_FIELDS = (
    "Age",
    "Weight",
    "Height",
    "NCIC",
    "Eyes",
    "Hair",
    "Complexion",
    "Languages",
    "Build",
    "Scars and Marks",
)
SPLIT_DATES = re.compile("([^,]+,[^,]+)")


def index_validator(doc: etree._Element) -> bool:
    return doc.find('.//div[@class="row top-total"]//p') is not None


def detail_Validator(doc: etree._Element) -> bool:
    return doc.find('.//h1[@class="documentFirstHeading"]') is not None


def crawl_person(context: Context, url: str) -> None:
    doc = fetch_html(context, url, detail_Validator, cache_days=7)
    name = doc.findtext('.//h1[@class="documentFirstHeading"]')
    if name is None:
        context.log.error("Cannot find name table", url=url)
        return
    table = doc.find('.//table[@class="table table-striped wanted-person-description"]')
    # Detect if the table with person information exists or do not make a person
    # Because sometimes they add also groups for example the whole gru group
    if table is None:
        context.log.debug("Cannot find fact table", url=url)
        return

    person = context.make("Person")
    person.id = context.make_slug(name)
    person.add("topics", "crime")
    person.add("name", name)
    person.add("sourceUrl", url)
    # last_name, first_name = name.split(" ", 1)
    # person.add("firstName", first_name)
    # person.add("lastName", last_name)

    # Add aditional information
    rows = table.findall(".//tr")
    for item in rows:
        cells = [c.text.strip() for c in item.findall("./td")]  # type: ignore
        if len(cells) != 2:
            context.log.error("Invalid fact table entry", cells=cells)
            continue
        key, value = cells
        if value is None or not len(value.strip()):
            continue
        if "Nationality" in key:
            person.add("nationality", value)
        elif "Citizenship" in key:
            person.add("nationality", value.split(","))
        elif "Place of Birth" in key:
            person.add("birthPlace", value)
        elif "Occupation" in key:
            person.add("position", value)
        elif "Sex" in key:
            person.add("gender", value)
        elif "Race" in key:
            person.add("ethnicity", value)
        elif "Date(s) of Birth Used" in key:
            dates = SPLIT_DATES.split(value)
            for date in dates:
                date = date.replace("(True)", "").strip()
                if len(date) > 1:
                    person.add("birthDate", h.parse_date(date, FORMATS))
        elif key in IGNORE_FIELDS:
            note = "%s: %s" % (key, value)
            person.add("notes", note)
        else:
            context.log.warn("Unknown field in table", key=key, value=value)

    remarks = doc.findtext('.//div[@class="wanted-person-remarks"]/p')
    person.add("notes", remarks)

    caution = doc.findtext('.//div[@class="wanted-person-caution"]/p')
    person.add("notes", caution)

    # context.inspect(person)
    context.emit(person, target=True)


def crawl_type(context: Context, type: str, query_id: str):
    total_pages: Optional[int] = None
    for page in count(1):
        if total_pages is not None and page > total_pages:
            break
        url = FBI_URL % (type, query_id, page)
        # print(url)
        context.log.info("Fetching %s" % url)
        doc = fetch_html(context, url, index_validator)

        if total_pages is None:
            # Get total results count
            total_text = doc.findtext('.//div[@class="row top-total"]//p')
            if total_text is None:
                context.log.error("Could not find result count", url=url)
                continue
            # context.inspect(total_el)
            # total_text = total_el.text_content()
            match = re.search(r"\d+", total_text)
            if match is None:
                context.log.error("Could not find result count", url=url)
                continue
            total_results = int(match.group())
            context.log.debug("Total results", total_results=total_results, url=url)

            # Get total pages count
            total_pages = math.ceil(total_results / 40)

        details = doc.find('.//div[@class="query-results pat-pager"]')
        if details is None:
            context.log.error("Cannot find details", url=url)
            continue
        for row in details.findall(".//ul/li"):
            href: str = row.xpath(".//a")[0].get("href")  # type: ignore
            crawl_person(context, href)


def crawl(context: Context):
    crawl_type(context, "topten", "0f737222c5054a81a120bce207b0446a")
    crawl_type(context, "fugitives", "f7f80a1681ac41a08266bd0920c9d9d8")
    crawl_type(context, "terrorism", "55d8265003c84ff2a7688d7acd8ebd5a")
    crawl_type(context, "bank-robbers", "2514fe8f611f47d1b2c1aa18f0f6f01b")
