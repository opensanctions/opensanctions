# from urllib.parse import urljoin
# from lxml.etree import tostring
import re

from normality import collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


REGEX_CLEAN_NAME = re.compile(
    r"The Chief Justice|President of the Court of Appeal|Justice|Rt[\.\b]|The |Hon[\.\b]|\bLLB\b|\bBSc\b|\bLLD\b|\bCLE\b|\bKC\b|\bCBE\b|\(\w+\)|Sir|, "
)


def get_judges(context: Context, doc):
    tabs = doc.xpath(".//div[contains(@class, 'judicial-tabs')]//a")
    current_tab = [t for t in tabs if t.text == "Current"][0]
    current_pane_id = current_tab.get("data-bs-target")[1:]
    current_pane = doc.xpath(f".//div[@id='{current_pane_id}']")[0]
    return current_pane.xpath(".//div[contains(@class, 'col-md-6 col-lg-8')]")


# def get_name_pos(context: Context, container, default_position: str):
#     name_els = container.xpath(".//h3")
#     assert len(name_els) == 1, (name_els, tostring(container))
#     name = name_els[0].text_content()

#     if "chief justice" in name.lower():
#         position = "Chief Justice"
#     elif "president of the court of appeal" in name.lower():
#         position = "President of the Court of Appeal"
#     else:
#         position = default_position

#     name = REGEX_CLEAN_NAME.sub("", name)
#     name = collapse_spaces(name)
#     return name, position


def get_name_pos(container):
    name_el = container.xpath(".//h2[@class='tlp-member-title']")
    position_el = container.xpath(".//div[@class='tlp-position']")
    details_el = container.xpath(".//div[@class='tlp-member-detail']")

    name = name_el[0].text_content().strip()
    name = collapse_spaces(REGEX_CLEAN_NAME.sub("", name))
    # TODO: some positions should be extracted from the name element
    position = position_el[0].text_content().strip() if position_el else ""
    details = details_el[0].text_content().strip()

    return name, position, details


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
            doc = context.fetch_html(person_url, cache_days=1)
            containers = doc.xpath(
                '//div[contains(@class, "tlp-member-description-container")]'
            )
            for judge_container in containers:
                name, position, details = get_name_pos(judge_container)
                person_proxy = context.make("Person")
                person_proxy.id = context.make_id(name)
                person_proxy.add("sourceUrl", person_url)
                person_proxy.add("notes", details)
                h.apply_name(person_proxy, full=name)
                person_proxy.add("topics", "role.judge")

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


# def crawl_judges(context: Context, url, default_position, min, max):
#     doc = context.fetch_html(url, cache_days=30)
#     doc.make_links_absolute(url)

#     containers = get_judges(context, doc)
#     assert min <= len(containers) <= max, (min, len(containers), max)

#     for judge_container in containers:
#         name, position = get_name_pos(context, judge_container, default_position)
#         person_proxy = context.make("Person")
#         person_proxy.id = context.make_id(name)
#         person_proxy.add("sourceUrl", url)
#         h.apply_name(person_proxy, full=name)
#         person_proxy.add("topics", "role.judge")

#         position = h.make_position(
#             context,
#             name=position,
#             country="Cayman Islands",
#         )
#         categorisation = categorise(context, position, is_pep=True)
#         if not categorisation.is_pep:
#             continue
#         occupancy = h.make_occupancy(
#             context, person_proxy, position, True, categorisation=categorisation
#         )
#         if not occupancy:
#             continue
#         context.emit(person_proxy)
#         context.emit(position)
#         context.emit(occupancy)


# def crawl(context: Context):
#     crawl_judges(context, context.data_url, "Chief Justice", 1, 1)

#     appeal_court_url = urljoin(context.data_url, "court-of-appeal")
#     crawl_judges(context, appeal_court_url, "Justice of the Court of Appeal", 3, 10)

#     grand_court_url = urljoin(context.data_url, "grand-court")
#     crawl_judges(context, grand_court_url, "Judge of the Grand Court", 3, 10)
