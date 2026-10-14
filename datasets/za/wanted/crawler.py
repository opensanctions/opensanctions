from urllib.parse import parse_qs, urlparse

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract.zyte_api import fetch_html
from zavod.util import Element


UNKNOWNS = {"unknown", "uknown"}
# Cache detail pages for a day because it's useful for retries, but they also seem to
# list pages before they're fully populated which can break a crawl one day and not the next.
CACHE_DAYS = 1


def crawl_detail_page(context: Context, person: Entity, source_url: str) -> None:
    """Fetch and parse detailed information from a person's detail page."""
    doc = fetch_html(
        context,
        source_url,
        "//td[b[contains(text(), 'Crime:')]]",
        geolocation="za",
        cache_days=CACHE_DAYS,
    )

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
        key: (
            h.xpath_strings(doc, xpath)[0].strip()
            if h.xpath_strings(doc, xpath)
            else ""
        )
        for key, xpath in details.items()
    }
    status = doc.findtext(".//p[@align='center']/font[@color='blue']")
    # The source sometimes publishes listings without a status. These are incomplete, and it's usually only a few at a time.
    if not status:
        return
    if status not in {"Wanted", "Suspect"}:
        context.log.warning("Unknown status", status=status, url=source_url)
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

    person.add("notes", f"{status} - {info['crime']}")

    context.emit(person)


def crawl_person(context: Context, row: dict[str, Element]) -> None:
    detail_url = h.xpath_strings(row["Surname"], ".//a/@href")[0]

    # There can be additional text outside the link, e.g. "international sought"
    names_els = h.xpath_elements(row.pop("Name"), "./a")
    assert len(names_els) == 1, len(names_els)
    forenames = h.element_text(names_els[0], squash=False)
    forename_list = forenames.split(" ")

    last_name_els = h.xpath_elements(row.pop("Surname"), ".//a")
    assert len(last_name_els) == 1, len(last_name_els)
    last_name = h.element_text(last_name_els[0], squash=False)

    names = [last_name] + forename_list

    if any(n.lower() in UNKNOWNS for n in names):
        return

    person = context.make("Person")

    # each wanted person has a dedicated details page
    # which appears to be a unique identifier
    id = parse_qs(urlparse(detail_url).query)["bid"][0]
    person.id = context.make_slug(id)

    # name3 handles additional middle names
    h.apply_name(
        person,
        first_name=forename_list[0],
        middle_name=forename_list[1] if len(forename_list) > 1 else None,
        name3=forename_list[2] if len(forename_list) > 2 else None,
        last_name=last_name,
    )
    assert len(forename_list) <= 3, len(forename_list)

    person.add("sourceUrl", detail_url)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("country", "za")

    crawl_detail_page(context, person, detail_url)


def crawl(context: Context) -> None:
    doc = fetch_html(
        context,
        context.data_url,
        "//table",
        geolocation="za",
        # cache_days=1, Don't cache index pages. Cached links to deleted listings break the crawler.
        absolute_links=True,
    )
    table = h.xpath_element(doc, "//table")
    trs = h.xpath_elements(table, ".//tr")
    headers = [h.element_text(th) for th in h.xpath_elements(trs[2], ".//th")]
    for tr in trs[3:]:
        cells = [c for c in h.xpath_elements(tr, ".//*[self::td or self::th]")]
        if not cells:
            continue
        row = dict(zip(headers, cells))
        crawl_person(context, row)
