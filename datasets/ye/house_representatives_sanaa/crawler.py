import json
from itertools import count
from urllib.parse import urljoin

from lxml import etree, html
from zavod.extract import zyte_api
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import OccupancyStatus, categorise
from zavod.util import LangText

from zavod import Context
from zavod import helpers as h


def fetch_listing(context: Context, page: int) -> etree._Element:
    # The listing is paged through an ASP.NET web service that answers a JSON
    # POST with HTML member cards wrapped in {"d": ...}.
    res = zyte_api.fetch(
        context,
        zyte_api.ZyteAPIRequest(
            url=context.data_url,
            method="POST",
            body=json.dumps({"page": page, "secid": 0}).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
        ),
    )
    assert res.status_code == 200, (page, res.status_code)
    return html.fromstring(f"<div>{json.loads(res.response_text)['d']}</div>")


def crawl_person(
    context: Context, member_id: str, url: str, category: str, row: dict[str, str]
) -> None:
    name = row.pop("name")
    person = context.make("Person")
    person.id = context.make_slug(member_id)
    person.add("name", name, lang="ara")
    apply_translit_full_name(context, person, LangText(name, "ara"))
    dob = row.pop("birth_date")
    # 1 January stands in for an unknown day and month.
    if dob.endswith("/01/01"):
        h.apply_date(person, "birthDate", dob[:4], original_value=dob)
    else:
        h.apply_date(person, "birthDate", dob)
    person.add("birthPlace", row.pop("birth_place"), lang="ara")
    person.add("education", row.pop("education"), lang="ara")
    person.add("political", row.pop("political"), lang="ara")
    person.add("notes", row.pop("notes", None), lang="ara")
    person.add("sourceUrl", url)
    # Candidates must be Yemeni: Constitution of the Republic of Yemen, Art. 64.
    # https://www.constituteproject.org/constitution/Yemen_2015
    person.add("citizenship", "ye")

    constituency = f"{row.pop('constituency')} - {row.pop('governorate')}"

    res = context.lookup("position", category)
    assert res is not None, (url, category)
    for title in res.values:
        position = h.make_position(
            context,
            name=title,
            country="ye",
            topics=["gov.national", "gov.legislative"],
            lang="eng",
        )
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            # No dates, and it is unclear how promptly the site is updated.
            # Without a date, make_occupancy would drop everyone lacking a
            # birth date, so the status is set outright.
            status=OccupancyStatus.UNKNOWN,
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)

    context.audit_data(
        row,
        ignore=[
            # The post, committee and committee role restate the listing category
            # (committee chair or rapporteur), which is not a position of its own.
            "current_post",
            "committee",
            "committee_role",
            # Sub-division of the governorate already in the constituency.
            "district",
        ],
    )


def crawl_profile(context: Context, card: etree._Element) -> None:
    category = h.element_text(h.xpath_element(card, ".//div[@class='category']"))
    href = h.xpath_string(card, ".//div[@class='thumbnail']//a/@href")
    url = urljoin(context.data_url, f"/{href}")
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=".//div[@id='MainContent_personalinformation']",
        html_source="httpResponseBody",
    )
    # Each field is a "label: value" list item behind an icon.
    row: dict[str, str] = {}
    for li in h.xpath_elements(doc, ".//div[starts-with(@id, 'MainContent_')]/ul/li"):
        label, sep, value = h.element_text(li).partition(":")
        assert sep == ":", (url, label)
        key = context.lookup_value("columns", label.strip(), warn_unmatched=True)
        assert key is not None, (url, label)
        row[key] = value.strip()
    crawl_person(context, href.split("=")[-1], url, category, row)


def crawl(context: Context) -> None:
    for page in count(1):
        doc = fetch_listing(context, page)
        cards = h.xpath_elements(doc, ".//div[contains(@class, 'post-module-member')]")
        assert len(cards) > 0, page
        for card in cards:
            crawl_profile(context, card)
        # Every page but the last links to the next one as Members(<page>,0).
        next_links = h.xpath_strings(doc, ".//span[@aria-label='Next']/@onclick")
        if len(next_links) == 0:
            break
        assert next_links == [f"Members({page + 1},0)"], (page, next_links)
