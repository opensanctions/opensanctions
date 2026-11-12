import re
from uuid import uuid4
from collections.abc import Iterator
from urllib.parse import urlencode

from lxml import html
from zavod.extract import zyte_api
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.util import Element, LangText

from zavod import Context, Entity
from zavod import helpers as h

DETAIL_URL = "https://representatives.jo/Ar/pages/CouncilMembersDetails"
PRESIDENT_URL = "https://representatives.jo/Ar/Pages/CouncilPresident"


def fetch_html(context: Context, url: str, validator: str, cache_days: int) -> Element:
    # The government network only serves Jordanian IPs.
    return zyte_api.fetch_html(
        context,
        url,
        unblock_validator=validator,
        html_source="httpResponseBody",
        geolocation="JO",
        cache_days=cache_days,
    )


def fetch_pages(context: Context, url: str) -> Iterator[Element]:
    """Page through an ASP.NET listing by replaying the pager's postback for the
    link after the current page, until the pager reports the last page."""
    # The postbacks are checked against the session's anti-forgery cookie, which
    # the site only sets on the first page, so the whole listing goes through one
    # Zyte session (same IP, cookies kept).
    zyte_session = str(uuid4())
    request = zyte_api.ZyteAPIRequest(
        url=url, geolocation="JO", session_id=zyte_session
    )
    page = 1
    while True:
        result = zyte_api.fetch(context, request)
        assert result.status_code == 200, (url, result.status_code)
        doc = html.fromstring(result.response_text)
        yield doc
        pager = h.xpath_element(doc, ".//span[@id='MainContent_pagerList']")
        # "صفحة 3 من 12": page 3 of 12
        nav = h.element_text(h.xpath_element(pager, "./ancestor::nav[1]"))
        match = re.search(r"صفحة (\d+) من (\d+)", nav)
        assert match is not None and int(match.group(1)) == page, (page, nav)
        if page == int(match.group(2)):
            return
        # The link after the current page is the next page's number, or "..." for
        # the next block of page numbers.
        link = h.xpath_element(
            pager, "./span[@class='CurrentPageClass']/following-sibling::a[1]"
        )
        assert h.element_text(link) in (str(page + 1), "..."), h.element_text(link)
        target = re.fullmatch(
            r"javascript:__doPostBack\('([^']+)',''\)", link.get("href", "")
        )
        assert target is not None, link.get("href")
        data = {
            el.get("name", ""): el.get("value", "")
            for el in h.xpath_elements(doc, ".//form//input[@type='hidden']")
        }
        data["__EVENTTARGET"] = target.group(1)
        request = zyte_api.ZyteAPIRequest(
            url=url,
            method="POST",
            body=urlencode(data).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            geolocation="JO",
            session_id=zyte_session,
        )
        page += 1


def session_number(context: Context, session: str) -> int:
    number = context.lookup_value("sessions", session, warn_unmatched=True)
    assert number is not None, session
    return int(number)


def apply_name(context: Context, person: Entity, full_name: str) -> None:
    """Strip the rank and title before the name; the title gives the gender."""
    tokens = full_name.split()
    while (res := context.lookup("honorifics", tokens[0])) is not None:
        person.add("gender", res.gender)
        tokens.pop(0)
    name = " ".join(tokens)
    person.add("name", name)
    apply_translit_full_name(context, person, LangText(name, "ara"))


def crawl_member(
    context: Context,
    member_id: str,
    card_name: str,
    council: str,
    is_current: bool,
    member: tuple[Entity, PositionCategorisation],
    speaker: tuple[Entity, PositionCategorisation] | None,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(member_id)
    apply_name(context, person, card_name)
    # Members must be Jordanian: Constitution of Jordan, Art. 75(1)(a).
    # https://www.constituteproject.org/constitution/Jordan_2016
    person.add("citizenship", "jo")

    # Profiles of the previous House are empty unless the member was re-elected,
    # so they are only fetched for the sitting House.
    political_group = None
    if is_current:
        url = f"{DETAIL_URL}?RepresentativeID={member_id}"
        doc = fetch_html(context, url, ".//div[@id='info']", cache_days=7)
        row: dict[str, str] = {}
        for tr in h.xpath_elements(doc, ".//div[@id='info']//tr[count(td) = 2]"):
            label, value = [h.element_text(td) for td in h.xpath_elements(tr, "./td")]
            key = context.lookup_value(
                "columns", label.lstrip("- "), warn_unmatched=True
            )
            assert key is not None, (url, label)
            row[key] = value
        person.add("email", row.pop("email"))
        person.add("sourceUrl", url)
        # "<session> - <bloc>/<session> - <bloc>": the latest session's bloc.
        blocs = row.pop("blocs")
        if blocs != "لا يوجد":  # "none"
            entries = [b.split(" - ", 1) for b in blocs.split("/")]
            assert all(len(e) == 2 for e in entries), (url, blocs)
            latest = max(entries, key=lambda e: session_number(context, e[0]))
            political_group = latest[1]
        context.audit_data(
            row,
            ignore=[
                # The same name as on the member's card, which every member has.
                "name",
                # The listing's hfConsId already places each member in a House.
                "councils",
                "committees",
                "brotherhood_committees",
                "friendship_associations",
                # Phone numbers are not extracted for PEPs.
                "phone",
            ],
        )

    position, categorisation = member
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        # Terms are not dated: the sitting House is current, the one before it
        # has ended. Only one previous House is crawled, as ended occupancies
        # without dates never age out.
        status=None if is_current else OccupancyStatus.ENDED,
        key_prefix=council,
    )
    if occupancy is not None:
        occupancy.add("politicalGroup", political_group)
        context.emit(occupancy)
    if speaker is not None:
        position, categorisation = speaker
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            key_prefix=council,
        )
        if occupancy is not None:
            context.emit(occupancy)
    context.emit(person)


def crawl_council(
    context: Context,
    url: str,
    is_current: bool,
    member: tuple[Entity, PositionCategorisation],
    speaker: tuple[Entity, PositionCategorisation],
    speaker_id: str,
) -> tuple[int, set[str]]:
    """Crawl a House's listing; return its number (hfConsId) and member IDs."""
    council_ids: set[str] = set()
    member_ids: set[str] = set()
    # Fetch the whole listing before the slow member pages, so the pager's
    # postbacks follow each other closely enough to stay within the session.
    for doc in list(fetch_pages(context, url)):
        for card in h.xpath_elements(doc, ".//div[contains(@class, 'card ')]"):
            council_id = h.xpath_string(
                card, ".//input[contains(@name, 'hfConsId')]/@value"
            )
            council_ids.add(council_id)
            member_id = h.xpath_string(
                card, ".//input[contains(@name, 'hfRepresentativeID')]/@value"
            )
            member_ids.add(member_id)
            is_speaker = is_current and member_id == speaker_id
            name = h.element_text(h.xpath_element(card, ".//h6"))
            crawl_member(
                context,
                member_id,
                name,
                council_id,
                is_current,
                member,
                speaker if is_speaker else None,
            )
    assert len(council_ids) == 1, (url, council_ids)
    return int(council_ids.pop()), member_ids


def make_position(
    context: Context, name: str, wikidata_id: str | None
) -> tuple[Entity, PositionCategorisation]:
    position = h.make_position(
        context,
        name=name,
        wikidata_id=wikidata_id,
        country="jo",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    assert categorisation.is_pep, position.id
    context.emit(position)
    return position, categorisation


def crawl(context: Context) -> None:
    member = make_position(
        context, "Member of the House of Representatives of Jordan", "Q17524418"
    )
    speaker = make_position(
        context, "Speaker of the House of Representatives of Jordan", None
    )

    # The presidency page lists the Speaker of each session of the sitting House.
    doc = fetch_html(context, PRESIDENT_URL, ".//table//tbody/tr", cache_days=1)
    speakers: dict[int, str] = {}
    for tr in h.xpath_elements(doc, ".//table//tbody/tr"):
        session = session_number(
            context, h.element_text(h.xpath_element(tr, "./td[3]"))
        )
        assert session not in speakers, session
        speakers[session] = h.xpath_string(tr, ".//a/@href").rsplit("=", 1)[1]
    speaker_id = speakers[max(speakers)]

    # The plain listing shows the sitting House.
    current, members = crawl_council(
        context, context.data_url, True, member, speaker, speaker_id
    )
    assert speaker_id in members, speaker_id
    previous_url = f"{context.data_url}?CouncilID={current - 1}"
    previous, _ = crawl_council(
        context, previous_url, False, member, speaker, speaker_id
    )
    assert previous == current - 1, previous
