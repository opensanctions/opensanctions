from lxml.etree import _Element as Element
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member: Element,
) -> None:
    raw_name = h.xpath_string(member, ".//div[contains(@class, 'SubjectCSS')]//text()")
    name = h.strip_name_titles(context, raw_name)
    # Every name carries an honorific ending in "/"; one left over is a title missing
    # from names.prefixes_strip.
    assert name is not None and "/" not in name, raw_name
    emirate = h.element_text(
        h.xpath_element(member, ".//div[contains(@class, 'EMCSS')]")
    )

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add("name", name, original_value=raw_name)
    # Members must be citizens of an emirate: Constitution of the UAE, Art. 70.
    # https://www.constituteproject.org/constitution/United_Arab_Emirates_2009
    person.add("citizenship", "ae")

    # The roster only lists the sitting term and gives no dates, so membership
    # implies a current mandate.
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    # Members represent their emirate, e.g. "إمارة دبي".
    occupancy.add("constituency", emirate)
    context.emit(occupancy)
    context.emit(person)
    # Not extracted: the photo, and the list of terms served ("الفصل التشريعي"),
    # which has no dates to model the earlier terms with.


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
    # The page shows the 18th legislative term by default. A new term means new
    # members, so fail loudly when the default selection changes.
    term = h.xpath_string(
        doc, "//select[contains(@id, 'ddlTerms')]/option[@selected]/text()"
    )
    assert term == "18", term

    for member in h.xpath_elements(doc, "//div[@class='members-list']/ul/li"):
        crawl_member(context, position, categorisation, member)
