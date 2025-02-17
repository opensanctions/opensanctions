import re
from typing import Dict
from urllib.parse import parse_qs, urlparse

from lxml import html
from requests.exceptions import HTTPError

from zavod import Context
from zavod import helpers as h

REGEX_NAME_REASON_STATUS = re.compile(r"(.+)\((.+)\)(.+)")


def parse_detail_page(context: Context, source_url: str) -> Dict[str, str] | None:
    """Fetch and parse detailed information from a person's detail page."""
    try:
        doc = context.fetch_html(source_url, cache_days=1)
    except HTTPError as e:
        context.log.error("HTTP error getting details", url=source_url, error=str(e))
        return None

    # Extract details using XPath based on the provided HTML structure
    details = {
        # "crime": "//td[b[contains(text(), 'Crime:')]]/following-sibling::td/text()",
        "crime_circumstances": "//td[b[contains(text(), 'Crime Circumstances:')]]/following-sibling::td/p/text()",
        "crime_date": "//td[b[contains(text(), 'Crime Date:')]]/following-sibling::td/text()",
        "aliases": "//td[b[contains(text(), 'Aliases:')]]/following-sibling::td/text()",
        "gender": "//td[b[contains(text(), 'Gender:')]]/following-sibling::td/text()",
        "eye_color": "//td[b[contains(text(), 'Eye Colour:')]]/following-sibling::td/text()",
        "hair_color": "//td[b[contains(text(), 'Hair Colour:')]]/following-sibling::td/text()",
        "height": "//td[b[contains(text(), 'Height:')]]/following-sibling::td/text()",
        "weight": "//td[b[contains(text(), 'Weight:')]]/following-sibling::td/text()",
        # "build": "//td[b[contains(text(), 'Build:')]]/following-sibling::td/text()",
        # "station": "//td[b[contains(text(), 'Station:')]]/following-sibling::td/text()",
        # "case_number": "//td[b[contains(text(), 'Case Number:')]]/following-sibling::td/text()",
        # "station_tel": "//td[b[contains(text(), 'Station Telephone:')]]/following-sibling::td/text()",
        # "investigator": "//td[b[contains(text(), 'Investigating Officer:')]]/following-sibling::td/text()",
        # "investigator_contact": "//td[b[contains(text(), 'Contact nr:')]]/following-sibling::td/text()",
        # "investigator_email": "//td[b[contains(text(), 'E-mail:')]]/following-sibling::td/a/text()",
    }
    info = {
        key: (doc.xpath(xpath)[0].strip() if doc.xpath(xpath) else "")
        for key, xpath in details.items()
    }

    return info


def crawl_person(context: Context, cell: html.HtmlElement):
    source_url = cell.xpath(".//a/@href")[0]
    match = REGEX_NAME_REASON_STATUS.match(cell.text_content())

    if not match:
        context.log.warning("Regex did not match data for person %s" % source_url)
        return

    name, crime, status = map(str.strip, match.groups())

    # only emit a person if the name is not unknown
    unknown_spellings = ["Unknown", "Uknown", "unknown", "UNKNOWN"]
    if sum(name.count(x) for x in unknown_spellings) >= 1:
        return

    person = context.make("Person")

    # each wanted person has a dedicated details page
    # which appears to be a unique identifier
    id = parse_qs(urlparse(source_url).query)["bid"][0]
    person.id = context.make_slug(id)

    h.apply_name(person, full=name)

    person.add("sourceUrl", source_url)
    person.add("notes", f"{status} - {crime}")
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("country", "za")

    # Fetch and parse additional information from the detail page
    additional_info = parse_detail_page(context, source_url)
    if additional_info:
        if additional_info.get("aliases"):
            person.add("alias", additional_info["aliases"].split("; "))
        person.add("notes", additional_info.get("crime_circumstances"))
        person.add("gender", additional_info.get("gender"))
        person.add("eyeColor", additional_info.get("eye_color"))
        person.add("hairColor", additional_info.get("hair_color"))
        person.add("height", additional_info.get("height"))
        person.add("weight", additional_info.get("weight"))
    context.emit(person)


def crawl(context):
    doc = context.fetch_html(context.dataset.data.url, cache_days=1)
    # makes it easier to extract dedicated details page
    doc.make_links_absolute(context.dataset.data.url)
    cells = doc.xpath("//td[.//a[contains(@href, 'detail.php')]]")
    for cell in cells:
        crawl_person(context, cell)
