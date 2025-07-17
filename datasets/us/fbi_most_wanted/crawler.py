import re
import math
from itertools import count
from typing import Optional

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

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
SPLIT_DATES = re.compile(r"([^,]+,[^,]+)")


def crawl_person(context: Context, url: str) -> None:
    name_xpath = './/h1[@class="documentFirstHeading"]'
    doc = fetch_html(context, url, name_xpath, cache_days=7)
    name = doc.findtext(name_xpath)
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
    person.add("topics", "wanted")
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
            person.add("citizenship", value.split(","))
        elif "Place of Birth" in key:
            person.add("birthPlace", value)
        elif "Occupation" in key:
            person.add("position", value)
        elif "Sex" in key:
            person.add("gender", value)
        elif "Race" in key:
            person.add("ethnicity", value)
        elif "Hair" in key:
            person.add("hairColor", value)
        elif "Eyes" in key:
            person.add("eyeColor", value)
        elif "Height" in key:
            person.add("height", value)
        elif "Weight" in key:
            person.add("weight", value)
        elif "Scars and Marks" in key:
            person.add("appearance", value)
        elif "Date(s) of Birth Used" in key:
            dates = SPLIT_DATES.split(value)
            for date in dates:
                date = date.replace("(True)", "").strip()
                if len(date) > 1:
                    h.apply_date(person, "birthDate", date)
        elif key in IGNORE_FIELDS:
            note = "%s: %s" % (key, value)
            person.add("notes", note)
        else:
            context.log.warn("Unknown field in table", key=key, value=value)

    remarks = doc.findtext('.//div[@class="wanted-person-remarks"]/p')
    person.add("notes", remarks)

    caution = doc.findtext('.//div[@class="wanted-person-caution"]/p')
    person.add("notes", caution)

    aliases = doc.findtext('.//div[@class="wanted-person-aliases"]/p')
    if aliases is not None:
        aliases = aliases.replace(",”, “", "”, “")
        for raw_alias in aliases.split(","):
            alias = raw_alias.strip("”").strip()
            if alias:
                prop = "alias" if " " in alias else "weakAlias"
                person.add(prop, alias)

    # context.inspect(person)
    context.emit(person)


def crawl_type(context: Context, type: str, query_id: str):
    total_pages: Optional[int] = None
    total_xpath = './/div[@class="row top-total"]//p'
    for page in count(1):
        if total_pages is not None and page > total_pages:
            break
        url = FBI_URL % (type, query_id, page)
        # print(url)
        context.log.info("Fetching %s" % url)
        doc = fetch_html(context, url, total_xpath)

        if total_pages is None:
            # Get total results count
            total_text = doc.findtext(total_xpath)
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
    # Check if new menu items have been added that we potentially want to crawl.
    # Expected items listed below.
    menu_xpath = ".//ul[contains(@class, 'section-menu')]"
    doc = fetch_html(context, context.dataset.model.url, menu_xpath)
    [menu] = doc.xpath(menu_xpath)
    h.assert_dom_hash(menu, "8c4476503900c45adeb407a980cbc7663688aa1f")

    crawl_type(context, "topten", "0f737222c5054a81a120bce207b0446a")
    crawl_type(context, "fugitives", "f7f80a1681ac41a08266bd0920c9d9d8")
    crawl_type(context, "terrorism", "55d8265003c84ff2a7688d7acd8ebd5a")
    # kidnap - these are the victims, not wanted criminals.
    crawl_type(context, "parental-kidnappings", "querylisting-1")
    # seeking-information - some of these are victims, some name unknown criminals
    # ecap - name unknown, not all suspects
    # indian-country - some victims
    # vicap - Looks like mostly victims, many name unknown
