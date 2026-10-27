from urllib.parse import urlencode

from lxml import html
from lxml.etree import _Element as Element
from zavod.extract import zyte_api
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.util import LangText

from zavod import Context, Entity
from zavod import helpers as h

TERM_SELECT = "//select[contains(@id, 'ddlTerms')]"


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: str,
    status: OccupancyStatus | None,
    member: Element,
) -> None:
    raw_name = h.element_text(
        h.xpath_element(member, ".//div[contains(@class, 'SubjectCSS')]")
    )
    name = h.strip_name_titles(context, raw_name)
    # Every name carries an honorific ending in "/"; one left over is a title missing
    # from names.prefixes_strip.
    if name is None or "/" in name:
        context.log.warning("Unknown honorific in member name", name=raw_name)
        return
    emirate = h.element_text(
        h.xpath_element(member, ".//div[contains(@class, 'EMCSS')]")
    )

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add("name", name, original_value=raw_name)
    apply_translit_full_name(context, person, LangText(name, "ara"))
    # Members must be citizens of an emirate: Constitution of the UAE, Art. 70.
    # https://www.constituteproject.org/constitution/United_Arab_Emirates_2009
    person.add("citizenship", "ae")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        status=status,
        # The source gives no dates, so the term keeps a re-elected member's two
        # occupancies apart.
        key_prefix=term,
    )
    if occupancy is None:
        return
    # Members represent their emirate, e.g. "إمارة دبي".
    occupancy.add("constituency", emirate)
    context.emit(occupancy)
    context.emit(person)
    # Not extracted: the photo, and the list of terms served ("الفصل التشريعي"),
    # which has no dates to model the earlier terms with.


def fetch_term(context: Context, doc: Element, value: str) -> Element:
    """Post the page's form back with the term `value` selected."""
    select_name = h.xpath_string(doc, f"{TERM_SELECT}/@name")
    form = {
        h.xpath_string(field, "./@name"): field.get("value", "")
        for field in h.xpath_elements(doc, "//form//input[@type='hidden']")
    }
    form.update(
        {select_name: value, "__EVENTTARGET": select_name, "__EVENTARGUMENT": ""}
    )
    result = zyte_api.fetch(
        context,
        zyte_api.ZyteAPIRequest(
            url=context.data_url,
            method="POST",
            body=urlencode(form).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            geolocation="AE",
        ),
    )
    term_doc = html.fromstring(result.response_text)
    # A postback that didn't switch terms returns the default term's members.
    shown = h.xpath_string(term_doc, f"{TERM_SELECT}/option[@selected]/@value")
    assert shown == value, (shown, value)
    return term_doc


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Federal National Council of the United Arab Emirates",
        wikidata_id="Q21328608",
        country="ae",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The site only answers requests from UAE/Gulf IP ranges.
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator="//div[@class='members-list']",
        html_source="httpResponseBody",
        geolocation="AE",
        cache_days=1,
    )
    # The page opens on the sitting term, the 18th. Flag a new one so a maintainer
    # confirms the page still lists the sitting term first.
    current = h.xpath_element(doc, f"{TERM_SELECT}/option[@selected]")
    if h.element_text(current) != "18":
        context.log.warning(
            "Default legislative term has changed", term=h.element_text(current)
        )
    # Step back by position: option values drift from their labels (label 15 is
    # value 16).
    previous = h.xpath_element(current, "./preceding-sibling::option[1]")
    previous_doc = fetch_term(context, doc, h.xpath_string(previous, "./@value"))

    # The source gives no dates: the default term is the sitting one, and members
    # of the term before it have since left that mandate.
    for option, term_doc, status in [
        (current, doc, None),
        (previous, previous_doc, OccupancyStatus.ENDED),
    ]:
        term = h.element_text(option)
        for member in h.xpath_elements(term_doc, "//div[@class='members-list']/ul/li"):
            crawl_member(context, position, categorisation, term, status, member)
