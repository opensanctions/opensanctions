import json
from urllib.parse import urlencode

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

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
    house_positions: dict[str, tuple[Entity, PositionCategorisation, str] | None],
    meta: dict[str, list[str]],
) -> None:
    (pk,) = meta.pop("memberId")
    profile_url = (
        "https://www.parliament.nsw.gov.au/members-and-electorates/"
        f"members-and-ministers/members-details?memberId={pk}"
    )
    (house,) = meta.pop("houseName")
    if house not in house_positions:
        context.log.warning("Unknown house code", house=house)
        return
    house_position = house_positions[house]
    if house_position is None:
        return
    position, categorisation, chamber = house_position

    # Electorate is only listed for Legislative Assembly members; Legislative
    # Council members are elected statewide and have no single electorate.
    constituency = meta.pop("electorate", None)

    # The listing has no term dates or biography; both live on the profile page.
    # Cloudflare bans Zyte plain HTTP fetches of profile pages (HTTP 520), but
    # lets browser-rendered fetches through.
    detail = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator="//div[contains(@class, 'pims-member-banner')]",
        html_source="browserHtml",
        cache_days=14,
    )
    start_date = extract_term_start(detail, chamber)
    biography = extract_biography(detail)

    person = context.make("Person")
    person.id = context.make_slug("member", pk)
    # `firstName` holds the full given names, `memberName` the name the member
    # goes by (e.g. "Jennifer Kathleen" vs "Jenny Aitchison").
    (first_name,) = meta.pop("firstName")
    (last_name,) = meta.pop("lastName")
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="eng")
    person.add("name", meta.pop("memberName"), lang="eng")
    person.add("political", meta.pop("party"))
    person.add("gender", meta.pop("gender"))
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
        occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(person)

    context.audit_data(
        meta,
        ignore=[
            # Search index bookkeeping, display variants of the name, and the photo.
            "d",
            "t",
            "globalSearchDisplayTitle",
            "surnameFilterKey",
            "seniority",
            "isCurrent",
            "photo",
            # Offices and term details beyond the membership and its start date.
            "portfolio",
            "currentMinistries",
            "currentOffices",
            "isMinister",
            "isShadowMinister",
            "isParliamentarySecretary",
            "termOfServiceExpiry",
        ],
    )


def crawl(context: Context) -> None:
    house_positions: dict[str, tuple[Entity, PositionCategorisation, str] | None] = {}
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
        if not categorisation.is_pep:
            house_positions[house_name] = None
            continue
        context.emit(position)
        house_positions[house_name] = (position, categorisation, config["chamber"])

    # The members listing is rendered client-side from this Funnelback search
    # index. `query` is the match-nothing-in-particular sentinel the site itself
    # sends to retrieve the unfiltered member list; `SF` selects the metadata
    # fields returned.
    search_params = {
        "collection": "pon1~sp-members",
        "profile": "members-current",
        "query": "!FunDoesNotExist:padrenull",
        "num_ranks": "500",
        "SF": "[.*]",
    }
    search_url = f"{context.data_url}?{urlencode(search_params)}"
    # Fetched with browser rendering, like the profile pages, which Cloudflare
    # bans for Zyte plain HTTP fetches. The browser wraps the JSON in a <pre>.
    doc = zyte_api.fetch_html(
        context,
        search_url,
        unblock_validator="//pre",
        html_source="browserHtml",
    )
    data = json.loads(h.element_text(h.xpath_element(doc, "//pre"), squash=False))
    packet = data["response"]["resultPacket"]
    results = packet["results"]
    # Guard against the listing being silently truncated by the page size.
    total = packet["resultsSummary"]["totalMatching"]
    if total != len(results):
        raise ValueError(f"Got {len(results)} of {total} members from the listing")
    for result in results:
        crawl_member(context, house_positions, result["listMetadata"])
