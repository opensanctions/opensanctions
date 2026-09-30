import re

from normality import collapse_spaces

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element


REGEX_CLEAN_NAME = re.compile(
    r"Justice|Rt[\.\b]|Hon[\.\b]|\bKC\b|\bCBE\b|Sir|\bHer\b|\bHis\b|\bMr[\.\b]|\bMrs[\.\b]|\bMs[\.\b]|\bKCMG\b|\bPC\b|"
)


def get_name_pos(container: Element, context: Context) -> tuple[str, str | None, str]:
    """Returns (name, position, details) for a judge container element."""
    name_el = h.xpath_elements(container, ".//h2[@class='tlp-member-title']")
    position_el = h.xpath_elements(container, ".//div[@class='tlp-position']")
    details_el = h.xpath_elements(container, ".//div[@class='tlp-member-detail']")

    name = h.element_text(name_el[0], squash=False).strip()
    # Check for the position_el for cases when position is in the same element as name
    # Only needed for chief justice at the moment
    position = h.element_text(position_el[0]) if position_el else None
    details = h.element_text(details_el[0])

    override_res = context.lookup("overrides", name)
    if override_res:
        name = override_res.name
        position = override_res.position
    elif "chief justice" in name.lower() or position is None:
        context.log.warning(f'No override found for "{name}" and "{position}"')

    clean_name = collapse_spaces(REGEX_CLEAN_NAME.sub("", name))
    assert clean_name is not None
    name = clean_name
    # Check for titles not captured by the regex
    word_count = len(name.split())
    if word_count >= 4:
        context.log.warning(
            f"Unexpectedly long name: {name}, additional cleanup might be needed"
        )
    return name, position, details


def crawl_page(context: Context, person_url: str) -> None:
    doc = context.fetch_html(person_url, cache_days=1)
    containers = h.xpath_elements(
        doc, '//div[contains(@class, "tlp-member-description-container")]'
    )
    for judge_container in containers:
        name, position, details = get_name_pos(judge_container, context)
        person_proxy = context.make("Person")
        person_proxy.id = context.make_id(name)
        h.apply_name(person_proxy, full=name)
        person_proxy.add("sourceUrl", person_url)
        person_proxy.add("biography", details)
        person_proxy.add("topics", "role.judge")

        # no citizenship requirements for judges:
        # https://gov.ky/documents/35692/0/Grand+Court+Act+(2026+Revision),++(1).pdf/57ca50f7-e129-6bf1-6cbf-69beb5991577
        # Section 6.2
        person_proxy.add("country", "ky")

        position_entity = h.make_position(
            context,
            name=position,
            country="Cayman Islands",
            topics=["gov.national", "gov.judicial"],
        )
        categorisation = categorise(context, position_entity, default_is_pep=True)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person_proxy, position_entity, True, categorisation=categorisation
        )
        if not occupancy:
            continue
        context.emit(person_proxy)
        context.emit(position_entity)
        context.emit(occupancy)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    profile_links = [
        link
        for link in h.xpath_strings(doc, '//ul[@id="menu-judicial-officers"]//a/@href')
        if link != "#"
    ]
    assert len(profile_links) >= 6, profile_links
    for url in profile_links:
        doc = context.fetch_html(url, cache_days=1)
        person_urls = h.xpath_strings(
            doc, '//div[@class="single-team-area"]//a[@class="rt-ream-me-btn"]/@href'
        )
        for person_url in person_urls:
            crawl_page(context, person_url)
