import re

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.extract.zyte_api import fetch_html
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

DEPUTY_LINK_XPATH = "//a[contains(@href, 'single-deputy/')]"
# Deputy profile links look like ``single-deputy/168?lang=en``; the number is the
# stable source id we key the person entity on.
DEPUTY_RE = re.compile(r"single-deputy/(\d+)")


def parse_deputy_links(doc: Element) -> dict[str, str]:
    """Map the id of each deputy linked from the convocation roster to their profile."""
    links: dict[str, str] = {}
    for href in h.xpath_strings(doc, f"{DEPUTY_LINK_XPATH}/@href"):
        match = DEPUTY_RE.search(href)
        assert match is not None, href
        links.setdefault(match.group(1), href)
    return links


def parse_profile_fields(right_block: Element) -> dict[str, Element]:
    """Read the profile's ``label:`` rows, keyed by label without the colon.

    The profile is a vertical label/value list rather than a column-oriented table,
    so `h.parse_html_table()` doesn't fit. The values keep their markup: both the
    constituency and the biography are structured inside their own value block.
    """
    fields: dict[str, Element] = {}
    for row in h.xpath_elements(
        right_block, ".//div[contains(@class, 'key_value_wrapper')]"
    ):
        label = h.element_text(h.xpath_element(row, ".//span[@class='key']"))
        value = h.xpath_element(row, ".//*[contains(@class, 'value')]")
        fields[label.rstrip(":")] = value
    return fields


def crawl_deputy(
    context: Context,
    url: str,
    deputy_id: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    right_block_xpath = "//div[contains(@class, 'right_block')]"
    doc = fetch_html(
        context,
        url,
        unblock_validator=right_block_xpath,
        html_source="httpResponseBody",
        geolocation="TM",
        cache_days=7,
    )
    right_block = h.xpath_element(doc, right_block_xpath)
    fields = parse_profile_fields(right_block)

    person = context.make("Person")
    person.id = context.make_slug("deputy", deputy_id)
    name = h.element_text(h.xpath_element(right_block, ".//h3[@class='name']"))
    h.apply_name(person, full=name, lang="eng")
    person.add("sourceUrl", url)
    # Deputies of the Mejlis must be citizens of Turkmenistan (Constitution art. 120).
    # https://www.constituteproject.org/constitution/Turkmenistan_2016
    person.add("citizenship", "tm")
    h.apply_date(person, "birthDate", h.element_text(fields.pop("Year of Birth")))

    biography = fields.pop("Biography")
    paragraphs = h.xpath_elements(biography, ".//div[@class='deputy_bio_text']//p")
    person.add(
        "biography",
        "\n".join(text for text in map(h.element_text, paragraphs) if len(text) > 0),
    )

    # The value block spells the district out in Turkmen at length; the span holds just
    # its name and number, e.g. "95th «Garlyk» election district".
    district = h.xpath_element(
        fields.pop("Constituency"), ".//span[@class='district_name']"
    )
    # "Position" holds an outside job title for some deputies and their committee role
    # for others, so it maps to neither cleanly. A committee seat is a position of its
    # own, which this crawler doesn't model.
    context.audit_data(fields, ignore=["Position", "Committee"])

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", h.element_text(district))

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Mejlis of Turkmenistan",
        country="tm",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328577",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    doc = fetch_html(
        context,
        f"{context.data_url}?lang=en",
        unblock_validator=DEPUTY_LINK_XPATH,
        html_source="httpResponseBody",
        geolocation="TM",
        cache_days=1,
    )
    deputies = parse_deputy_links(doc)
    assert len(deputies) > 0, context.data_url

    for deputy_id, url in deputies.items():
        crawl_deputy(context, url, deputy_id, position, categorisation)
