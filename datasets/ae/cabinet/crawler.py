import re

from lxml import html
from lxml.etree import _Element as Element
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

from zavod import Context, Entity
from zavod import helpers as h

NEXT_PAGE = re.compile(r'var nextPage = "(.*?)";')
# Honorifics are stripped via names.prefixes_strip; one left over is missing there.
HONORIFIC = re.compile(r"\b(Highness|Excellency|Sheikha?)\b", re.IGNORECASE)


def fetch_list(context: Context, url: str) -> Element:
    # The site only answers requests from UAE IP ranges.
    return zyte_api.fetch_html(
        context,
        url,
        unblock_validator="//div[contains(@class, 'cabItem')]",
        html_source="httpResponseBody",
        geolocation="AE",
        cache_days=1,
    )


def fetch_popup(context: Context, url: str) -> Element:
    # The page's script loads the popup over AJAX, and the endpoint only serves
    # requests marked as such. fetch_html can't set headers, so the response is
    # checked and cached here instead.
    result = zyte_api.fetch(
        context,
        zyte_api.ZyteAPIRequest(
            url=url,
            headers={"X-Requested-With": "XMLHttpRequest"},
            geolocation="AE",
        ),
        cache_days=7,
    )
    doc = html.fromstring(result.response_text)
    popups = h.xpath_elements(doc, "//div[contains(@class, 'popupBox')]")
    assert len(popups) == 1, (url, result.status_code)
    if not result.from_cache:
        context.cache.set(result.cache_fingerprint, result.response_text)
    return doc


def role_positions(context: Context, role: str) -> list[str]:
    res = context.lookup("position", role)
    if res is not None:
        return res.values
    # A role joining several offices needs its split spelled out in the lookup.
    if "," in role or " and " in role:
        context.log.warning("Role needs a position lookup entry", role=role)
        return []
    return [role]


def crawl_member(
    context: Context,
    card: Element,
    is_minister: bool,
    extra_positions: list[Entity],
) -> None:
    raw_name = h.element_text(h.xpath_element(card, ".//a[@class='name_']"))
    name = h.strip_name_titles(context, raw_name)
    assert name is not None, raw_name
    if HONORIFIC.search(name):
        context.log.warning("Name keeps an honorific", name=raw_name)
    role = h.element_text(h.xpath_element(card, ".//div[@class='dec_']"))
    # The card links to a popup holding the member's full biography; the card
    # itself shows at most a teaser of it.
    popup_url = h.xpath_string(card, ".//a[@class='name_']/@href")
    popup = fetch_popup(context, popup_url)
    # Line breaks inside a paragraph would otherwise run sentences together.
    for br in h.xpath_elements(popup, "//br"):
        br.tail = "\n" + (br.tail or "")
    paragraphs = h.xpath_elements(popup, "//div[contains(@class, 'textBox')]//p")
    texts = [h.element_text(p) for p in paragraphs]
    bio = "\n".join(text for text in texts if text != "")

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    person.add("biography", bio)
    person.add("sourceUrl", popup_url)
    if is_minister:
        # Ministers must be UAE citizens: Constitution of the UAE, Art. 56.
        # https://www.constituteproject.org/constitution/United_Arab_Emirates_2009
        person.add("citizenship", "ae")
    else:
        # The Federal Supreme Council's members are the emirates' rulers (Art. 46);
        # the constitution sets no citizenship requirement for them.
        person.add("country", "ae")

    positions = [
        h.make_position(context, name=title, country="ae", lang="eng")
        for title in role_positions(context, role)
    ]
    for position in positions + extra_positions:
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        # The rosters list sitting members only and give no dates.
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(person)


def crawl(context: Context) -> None:
    url = context.data_url
    while url != "":
        doc = fetch_list(context, url)
        for card in h.xpath_elements(doc, "//div[contains(@class, 'cabItem')]"):
            crawl_member(context, card, is_minister=True, extra_positions=[])
        # The list loads 12 members at a time on scroll; the script carries the URL
        # of the next page, which is empty on the last one.
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
        crawl_member(context, card, is_minister=False, extra_positions=[council])
