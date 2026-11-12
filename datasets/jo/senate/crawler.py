import re
from uuid import uuid4
from collections.abc import Iterator
from urllib.parse import urlencode, urljoin

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

# "عمان – 1/7/1966", "27/1/1969" or "عمان - 22 نيسان 1952": an optional place of
# birth, then the date, with the month as a number or a Levantine month name.
BIRTH = re.compile(
    r"(?:(?P<place>[^\d–-]+?)\s*[–-]\s*)?"
    r"(?P<date>\d{1,2}/\d{1,2}/\d{4}|\d{1,2} [^\d]+? \d{4})"
)


def fetch_pages(context: Context, url: str) -> Iterator[Element]:
    """Page through an ASP.NET listing by replaying the pager's postback for the
    link after the current page, until the pager reports the last page."""
    # The government network only serves Jordanian IPs. The postbacks are checked
    # against the session cookie, which the site only sets on the first page, so the
    # whole listing goes through one Zyte session (same IP, cookies kept).
    zyte_session = str(uuid4())
    request = zyte_api.ZyteAPIRequest(
        url=url, geolocation="JO", session_id=zyte_session
    )
    page = 1
    while True:
        result = zyte_api.fetch(context, request)
        assert result.status_code == 200, (url, result.status_code)
        doc = html.fromstring(result.response_text)
        pagers = h.xpath_elements(doc, ".//span[@id='pagerList']")
        if len(pagers) != 1:
            title = h.xpath_strings(doc, ".//title/text()")
            raise ValueError(f"No pager on listing page {page}: {title!r}")
        pager = pagers[0]
        yield doc
        # "صفحة 3 من 8": page 3 of 8
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
        data["__EVENTARGUMENT"] = ""
        request = zyte_api.ZyteAPIRequest(
            url=url,
            method="POST",
            body=urlencode(data).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            geolocation="JO",
            session_id=zyte_session,
        )
        page += 1


def strip_honorifics(context: Context, person: Entity, full_name: str) -> str:
    """Strip the rank and title before a name; the title gives the gender."""
    tokens = full_name.split()
    while (res := context.lookup("honorifics", tokens[0])) is not None:
        person.add("gender", res.gender)
        tokens.pop(0)
    return " ".join(tokens)


def cv_field(doc: Element, label: str) -> str | None:
    """Return the paragraph after a CV heading such as "مكان وتاريخ الولادة:"."""
    values = h.xpath_elements(
        doc, f".//*[self::h5 or self::p][normalize-space() = '{label}']/following::p[1]"
    )
    if len(values) == 0:
        return None
    assert len(values) == 1, label
    return h.element_text(values[0])


def crawl_member(
    context: Context,
    url: str,
    label: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=".//div[@id='MainContent_DivContent']",
        html_source="httpResponseBody",
        geolocation="JO",
        cache_days=7,
    )

    person = context.make("Person")
    person.id = context.make_slug(url.rsplit("/", 1)[1])
    name = strip_honorifics(context, person, label.lstrip("* "))
    person.add("name", name, lang="ara")
    apply_translit_full_name(context, person, LangText(name, "ara"))
    # "الاسم والشهرة": the name the senator is commonly known by.
    known_as = cv_field(doc, "الاسم والشهرة:")
    if known_as is not None:
        person.add("alias", strip_honorifics(context, person, known_as), lang="ara")
    birth = cv_field(doc, "مكان وتاريخ الولادة:")
    if birth is not None:
        match = BIRTH.fullmatch(birth)
        if match is None:
            context.log.warning(
                "Cannot parse place and date of birth", url=url, value=birth
            )
        else:
            h.apply_date(person, "birthDate", match.group("date"))
            person.add("birthPlace", match.group("place"), lang="ara")
    for email in h.xpath_strings(
        doc,
        ".//div[@id='MainContent_DivContent']//a[starts-with(@href, 'mailto:')]/@href",
    ):
        person.add("email", email.removeprefix("mailto:"))
    person.add("sourceUrl", url)
    # Senators must be Jordanian: Constitution of Jordan, Art. 75(1)(a).
    # https://www.constituteproject.org/constitution/Jordan_2016
    person.add("citizenship", "jo")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        # The roster is the decree that appointed the sitting Senate. Members who
        # have since left are marked with an asterisk, and those appointed later
        # are only named in a note, so neither carries a date.
        status=OccupancyStatus.ENDED if label.startswith("*") else None,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Jordan",
        wikidata_id="Q20081407",
        country="jo",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Fetch the whole listing before the slow member pages, so the pager's
    # postbacks follow each other closely enough to stay within the session.
    for doc in list(fetch_pages(context, context.data_url)):
        for card in h.xpath_elements(doc, ".//div[@id='divHeight']"):
            label = h.element_text(h.xpath_element(card, ".//h6"))
            url = h.xpath_string(card, ".//a[@id='lnkMore']/@href")
            crawl_member(
                context,
                urljoin(context.data_url, url),
                label,
                position,
                categorisation,
            )
