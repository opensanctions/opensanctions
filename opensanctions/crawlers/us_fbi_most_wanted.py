import re
import math

from opensanctions import helpers as h
from opensanctions.core import Context

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

types = {
    "fugitives": "f7f80a1681ac41a08266bd0920c9d9d8",
    "terrorism": "55d8265003c84ff2a7688d7acd8ebd5a",
    "bank-robbers": "2514fe8f611f47d1b2c1aa18f0f6f01b",
}


def crawl_person(context: Context, url: str) -> None:
    doc = context.fetch_html(url, cache_days=7)
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
    person.add("topics", "crime")
    person.add("name", name)
    person.id = context.make_slug(name)
    person.add("sourceUrl", url)
    # last_name, first_name = name.split(" ", 1)
    # person.add("firstName", first_name)
    # person.add("lastName", last_name)

    # Add aditional information
    rows = table.findall(".//tr")
    for item in rows:
        cells = [c.text.strip() for c in item.findall("./td")]
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
            continue
        else:
            context.inspect(item)
    context.emit(person, target=True)


def crawl_pages(context: Context, type: str, total_pages: int):
    # Crawl every page
    for page in range(1, total_pages + 1):
        page_url = FBI_URL % (type, types.get(type), page)
        doc = context.fetch_html(page_url)
        details = doc.find('.//div[@class="query-results pat-pager"]')
        for row in details.findall(".//ul/li"):
            href = row.xpath(".//a")[0].get("href")
            crawl_person(context, href)


def crawl(context: Context):
    for type in types:
        url = FBI_URL % (type, types[type], 1)
        doc = context.fetch_html(url)

        # Get total results count
        total_text = doc.findtext('.//div[@class="row top-total"]//p')
        if total_text is None:
            context.log.error("Could not find result count", url=url)
            continue
        # context.inspect(total_el)
        # total_text = total_el.text_content()
        match = re.search(r"\d+", total_text)
        if match is None:
            context.log.error("Could not find result count", text=total_text)
            continue
        total_results = int(match.group())
        context.log.debug("Total results", total_results=total_results, url=url)

        # Get total pages count
        total_pages = math.ceil(total_results / 40)
        crawl_pages(context, type, total_pages)
