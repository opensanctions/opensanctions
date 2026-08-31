import re
from typing import Any
from urllib.parse import urlencode

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

BASE_URL = "https://www.parliament.nsw.gov.au"
MEMBER_URL = BASE_URL + "/members-and-electorates/members-and-ministers/members-details"
# The members listing is rendered client-side from this Funnelback search index.
# `query` is the match-nothing-in-particular sentinel the site itself sends to
# retrieve the unfiltered member list; `SF` picks the metadata fields returned
# (without it the response carries base64 portrait photos and no gender).
SEARCH_PARAMS = {
    "collection": "pon1~sp-members",
    "profile": "members-current",
    "query": "!FunDoesNotExist:padrenull",
    "num_ranks": "500",
    "SF": "[memberName,lastName,gender,houseName,party,electorate]",
}
# The index URL of each result is the only place the member's numeric ID appears.
INDEX_URL_PK = re.compile(r"/member/(\d+)$")
MEMBER_BANNER_XPATH = "//div[contains(@class, 'pims-member-banner')]"

POSITIONS: dict[str, dict[str, str]] = {
    "Legislative Assembly": {
        "name": "Member of the New South Wales Legislative Assembly",
        "wikidata_id": "Q19202748",
        # The chamber word as it appears in the detail-page position tables.
        "chamber": "Assembly",
    },
    "Legislative Council": {
        "name": "Member of the New South Wales Legislative Council",
        "wikidata_id": "Q18810377",
        "chamber": "Council",
    },
}


def extract_term_start(detail: Element, chamber: str) -> str | None:
    """Return the date the member's current term in the given chamber began.

    The start date lives in the member's current-positions table on their
    profile page. The prior-positions table on the same page has the same CSS
    class, so we match on the header columns instead.
    """
    target = f"Member of the NSW Legislative {chamber}"
    for table in h.xpath_elements(detail, "//table"):
        rows = h.xpath_elements(table, ".//tr")
        if not rows:
            continue
        header = [h.element_text(c) for c in h.xpath_elements(rows[0], "./th | ./td")]
        if header[:3] != ["Position", "Start", "Notes"]:
            continue
        for row in rows[1:]:
            # The position name is a row header, the remaining columns are cells.
            cells = h.xpath_elements(row, "./th | ./td")
            if len(cells) < 2:
                continue
            if h.element_text(cells[0]) == target:
                return h.element_text(cells[1]) or None
    return None


def extract_biography(detail: Element) -> str | None:
    """Return the member's biography, one titled section per paragraph.

    The biography is split into sections (party activity, community activity,
    etc.); empty or hidden sections are skipped.
    """
    sections: list[str] = []
    xpath = "//div[contains(@class, 'pims-member-biography')]//section[contains(@class, 'pims-member-biography__block')]"
    for par in h.xpath_elements(detail, xpath):
        # Items within a section are separated by <br>, which text_content()
        # would otherwise run together ("...present.1992..."); insert spacing.
        for br in h.xpath_elements(par, ".//br"):
            br.tail = " " + (br.tail or "")
        titles = h.xpath_elements(
            par, ".//*[contains(@class, 'pims-member-biography__subheading')]"
        )
        title = h.element_text(titles[0]) if titles else ""
        body = " ".join(
            h.element_text(p) for p in h.xpath_elements(par, ".//p")
        ).strip()
        if not body:
            continue
        sections.append(f"{title}: {body}" if title else body)
    if not sections:
        return None
    return "\n\n".join(sections)


def crawl_member(
    context: Context,
    house_positions: dict[str, tuple[Entity, PositionCategorisation, str]],
    result: dict[str, Any],
) -> None:
    meta = result["listMetadata"]
    index_url = result["indexUrl"]
    match = INDEX_URL_PK.search(index_url)
    if match is None:
        context.log.warning("Unexpected member index URL", index_url=index_url)
        return
    pk = match.group(1)
    profile_url = f"{MEMBER_URL}?memberId={pk}"

    # The listing carries the full name and the surname, but not the given name.
    full_name = meta["memberName"][0]
    last_name = meta["lastName"][0]
    if not full_name.endswith(last_name):
        context.log.warning(
            "Surname is not a suffix of the full name",
            name=full_name,
            last_name=last_name,
        )
        return
    first_name = full_name[: -len(last_name)].strip()

    house = meta["houseName"][0]
    party = meta["party"][0] if meta.get("party") else None
    gender = meta["gender"][0] if meta.get("gender") else None

    if house not in house_positions:
        context.log.warning("Unknown house code", house=house)
        return

    position, categorisation, chamber = house_positions[house]

    # Electorate is only listed for Legislative Assembly members; Legislative
    # Council members are elected statewide and have no single electorate.
    electorates = meta.get("electorate", [])
    constituency = electorates[0] if electorates else None

    # The listing has no term dates or biography; both live on the profile page.
    # Cloudflare
    detail = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator=MEMBER_BANNER_XPATH,
        html_source="httpResponseBody",
        cache_days=14,
    )
    start_date = extract_term_start(detail, chamber)
    biography = extract_biography(detail)

    person = context.make("Person")
    person.id = context.make_slug("member", pk)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="eng")
    person.add("political", party)
    person.add("gender", gender)
    person.add("sourceUrl", profile_url)
    person.add("biography", biography)
    # Candidates must be enrolled to vote; enrolment requires Australian
    # citizenship: Electoral Act 2017 (NSW), ss 30 and 83.
    # https://legislation.nsw.gov.au/view/html/inforce/current/act-2017-066
    person.add("citizenship", "au")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
    )
    if occupancy is not None:
        if constituency is not None:
            occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    house_positions: dict[str, tuple[Entity, PositionCategorisation, str]] = {}
    for house_name, config in POSITIONS.items():
        position = h.make_position(
            context,
            name=config["name"],
            country="au",
            wikidata_id=config["wikidata_id"],
            topics=["gov.state", "gov.legislative"],
            lang="eng",
        )
        categorisation = categorise(context, position)
        context.emit(position)
        house_positions[house_name] = (position, categorisation, config["chamber"])

    search_url = f"{context.data_url}?{urlencode(SEARCH_PARAMS)}"
    data = zyte_api.fetch_json(context, search_url)  # Cloudflare
    packet = data["response"]["resultPacket"]
    results = packet["results"]
    # Guard against the listing being silently truncated by the page size.
    total = packet["resultsSummary"]["totalMatching"]
    if total != len(results):
        raise ValueError(f"Got {len(results)} of {total} members from the listing")
    for result in results:
        crawl_member(context, house_positions, result)
