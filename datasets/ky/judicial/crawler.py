import re

from normality import collapse_spaces

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


REGEX_CLEAN_NAME = re.compile(
    r"Justice|Rt[\.\b]|Hon[\.\b]|\bKC\b|\bCBE\b|Sir|\bHer\b|\bHis\b|\bMr[\.\b]|\bMrs[\.\b]|\bMs[\.\b]|\bKCMG\b|\bPC\b|"
)


def get_name_pos(container, context: Context):
    name_el = container.xpath(".//h2[@class='tlp-member-title']")
    position_el = container.xpath(".//div[@class='tlp-position']")
    details_el = container.xpath(".//div[@class='tlp-member-detail']")

    name = name_el[0].text_content().strip()
    # Check for the position_el for cases when position is in the same element as name
    # Only needed for chief justice at the moment
    position = position_el[0].text_content().strip() if position_el else None
    details = details_el[0].text_content().strip()

    if "chief justice" in name.lower():
        override_res = context.lookup("overrides", name)
        if override_res:
            name = override_res.name
            position = override_res.position
        else:
            context.log.warning(f'No override found for "{name}" and "{position}"')

    name = collapse_spaces(REGEX_CLEAN_NAME.sub("", name))
    # Check for titles not captured by the regex
    word_count = len(name.split())
    if word_count >= 4:
        context.log.warning(
            f"Unexpectedly long name: {name}, additional cleanup might be needed"
        )

    return name, position, details


def crawl_page(context: Context, person_url):
    doc = context.fetch_html(person_url, cache_days=1)
    containers = doc.xpath(
        '//div[contains(@class, "tlp-member-description-container")]'
    )
    for judge_container in containers:
        name, position, details = get_name_pos(judge_container, context)
        person_proxy = context.make("Person")
        person_proxy.id = context.make_id(name)
        h.apply_name(person_proxy, full=name)
        person_proxy.add("sourceUrl", person_url)
        person_proxy.add("notes", details)
        person_proxy.add("topics", "role.judge")
        # Check for cases when position is in the same element as name
        if position is None:
            context.log.warning(f"Position not found for {name}")
        position = h.make_position(
            context,
            name=position,
            country="Cayman Islands",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person_proxy, position, True, categorisation=categorisation
        )
        if not occupancy:
            continue
        context.emit(person_proxy)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    profile_links = [
        link
        for link in doc.xpath('//ul[@id="menu-judicial-officers"]//a/@href')
        if link != "#"
    ]
    assert len(profile_links) >= 6, profile_links
    for url in profile_links:
        doc = context.fetch_html(url, cache_days=1)
        profile_links = doc.xpath(
            '//div[@class="single-team-area"]//a[@class="rt-ream-me-btn"]/@href'
        )
        for person_url in profile_links:
            crawl_page(context, person_url)
