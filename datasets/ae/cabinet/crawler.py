import re

from lxml import html
from lxml.etree import _Element as Element
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

from zavod import Context, Entity
from zavod import helpers as h

NEXT_PAGE = re.compile(r'var nextPage = "(.*?)";')
# Catches honorifics missing from names.prefixes_strip.
HONORIFIC = re.compile(r"\b(Highness|Excellency|Sheikha?)\b", re.IGNORECASE)


def fetch_list(context: Context, url: str) -> Element:
    # The site only answers UAE IPs.
    return zyte_api.fetch_html(
        context,
        url,
        unblock_validator="//div[contains(@class, 'cabItem')]",
        html_source="httpResponseBody",
        geolocation="AE",
        cache_days=1,
    )


def fetch_popup(context: Context, url: str) -> Element:
    # The popup endpoint only serves AJAX requests, and fetch_html can't set
    # headers, so the response is validated and cached here.
    result = zyte_api.fetch(
        context,
        zyte_api.ZyteAPIRequest(
            url=url,
            headers={"X-Requested-With": "XMLHttpRequest"},
            geolocation="AE",
        ),
        cache_days=7,
    )
    popup = h.xpath_element(
        html.fromstring(result.response_text), "//div[contains(@class, 'popupBox')]"
    )
    if not result.from_cache:
        context.cache.set(result.cache_fingerprint, result.response_text)
    return popup


def role_positions(context: Context, role: str) -> list[str]:
    res = context.lookup("position", role)
    if res is not None:
        return res.values
    if "," in role or " and " in role:
        context.log.warning("Role needs a position lookup entry", role=role)
        return []
    return [role]


def crawl_member(
    context: Context, card: Element, membership: Entity, is_minister: bool
) -> None:
    link = h.xpath_element(card, ".//a[@class='name_']")
    raw_name = h.element_text(link)
    name = h.strip_name_titles(context, raw_name)
    assert name is not None, raw_name
    if HONORIFIC.search(name):
        context.log.warning("Name keeps an honorific", name=raw_name)
    role = h.element_text(h.xpath_element(card, ".//div[@class='dec_']"))
    popup_url = h.xpath_string(link, "./@href")
    popup = fetch_popup(context, popup_url)
    # Keep <br> from gluing words together.
    for br in h.xpath_elements(popup, ".//br"):
        br.tail = "\n" + (br.tail or "")
    texts = [
        h.element_text(p)
        for p in h.xpath_elements(popup, ".//div[contains(@class, 'textBox')]//p")
    ]

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    person.add("biography", "\n".join(text for text in texts if text != ""))
    person.add("sourceUrl", popup_url)

    if is_minister:
        # Ministers must be UAE citizens (Constitution, Art. 56):
        # https://www.constituteproject.org/constitution/United_Arab_Emirates_2009
        person.add("citizenship", "ae")
    else:
        # Council members are the emirates' rulers (Art. 46); no citizenship rule.
        person.add("country", "ae")

    positions = [
        h.make_position(context, name=title, country="ae", lang="eng")
        for title in role_positions(context, role)
    ]
    for position in [membership] + positions:
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        # The rosters list sitting members only, without dates.
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(person)


def crawl(context: Context) -> None:
    cabinet = h.make_position(
        context,
        name="Member of the Cabinet of the United Arab Emirates",
        country="ae",
        lang="eng",
    )
    url = context.data_url
    # The list loads 12 members per page; the last page has an empty nextPage.
    while url != "":
        doc = fetch_list(context, url)
        for card in h.xpath_elements(doc, "//div[contains(@class, 'cabItem')]"):
            crawl_member(context, card, cabinet, is_minister=True)
        script = h.xpath_string(doc, "//script[contains(., 'var nextPage')]/text()")
        match = NEXT_PAGE.search(script)
        assert match is not None, url
        url = match.group(1)

    council = h.make_position(
        context,
        name="Member of the Federal Supreme Council of the United Arab Emirates",
        country="ae",
        lang="eng",
    )
    doc = fetch_list(context, "https://uaecabinet.ae/en/federal-supreme-council")
    for card in h.xpath_elements(doc, "//div[contains(@class, 'cabItem')]"):
        crawl_member(context, card, council, is_minister=False)
