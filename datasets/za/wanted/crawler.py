from typing import Dict
from urllib.parse import parse_qs, urlparse

from lxml import html
from normality import collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity


UNKNOWNS = {"unknown", "uknown"}


def crawl_detail_page(context: Context, person: Entity, source_url: str):
    """Fetch and parse detailed information from a person's detail page."""
    doc = context.fetch_html(source_url, cache_days=7)

    # Extract details using XPath based on the provided HTML structure
    details = {
        "crime": "//td[b[contains(text(), 'Crime:')]]/following-sibling::td/text()",
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
    status = doc.findtext(".//p[@align='center']/font[@color='blue']")
    if status not in {"Wanted", "Suspect"}:
        context.log.warning("Unknown or missing status", status=status, url=source_url)
        status = None

    if info.get("aliases"):
        person.add(
            "alias",
            [a for a in info["aliases"].split("; ") if a.lower() not in UNKNOWNS],
        )
    person.add("notes", info.get("crime_circumstances"))
    person.add("gender", info.get("gender"))
    person.add("eyeColor", info.get("eye_color"))
    person.add("hairColor", info.get("hair_color"))
    person.add("height", info.get("height"))
    person.add("weight", info.get("weight"))

    person.add("notes", f"{status} - {info["crime"]}")

    context.emit(person)


def crawl_person(context: Context, row: Dict[str, html.HtmlElement]):
    detail_url = row["Surname"].xpath(".//a/@href")[0]

    # There can be additional text outside the link, e.g. "international sought"
    names_els = row.pop("Name").xpath("./a")
    assert len(names_els) == 1, len(names_els)
    forenames = names_els[0].text_content()
    forename_list = forenames.split(" ")

    last_name_els = row.pop("Surname").xpath(".//a")
    assert len(last_name_els) == 1, len(last_name_els)
    last_name = last_name_els[0].text_content()

    names = [last_name] + forename_list

    if any(n.lower() in UNKNOWNS for n in names):
        return

    person = context.make("Person")

    # each wanted person has a dedicated details page
    # which appears to be a unique identifier
    id = parse_qs(urlparse(detail_url).query)["bid"][0]
    person.id = context.make_slug(id)

    h.apply_name(
        person,
        first_name=forename_list[0],
        second_name=forename_list[1] if len(forename_list) > 1 else None,
        middle_name=forename_list[2] if len(forename_list) > 2 else None,
        last_name=last_name,
    )
    assert len(forename_list) <= 3, len(forename_list)

    person.add("sourceUrl", detail_url)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("country", "za")

    crawl_detail_page(context, person, detail_url)


def crawl(context):
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    tables = doc.xpath("//table")
    assert len(tables) == 1, len(tables)
    trs = tables[0].xpath(".//tr")
    headers = [collapse_spaces(h.text_content()) for h in trs[2].xpath(".//th")]
    for tr in trs[3:]:
        cells = [c for c in tr.xpath(".//*[self::td or self::th]")]
        if not cells:
            continue
        row = dict(zip(headers, cells))
        crawl_person(context, row)
