from urllib.parse import urljoin
from lxml.etree import tostring
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


def get_name_pos(context: Context, container, default_position: str):
    name_els = container.xpath(".//h3")
    assert len(name_els) == 1, (name_els, tostring(container))
    name = name_els[0].text_content()

    if "chief justice" in name.lower():
        position = "Chief Justice"
    elif "president of the court of appeal" in name.lower():
        position = "President of the Court of Appeal"
    else:
        position = default_position

    name = REGEX_CLEAN_NAME.sub("", name)
    name = collapse_spaces(name)
    return name, position


def crawl_judges(context: Context, url, default_position, min, max):
    doc = context.fetch_html(url, cache_days=30)
    doc.make_links_absolute(url)

    containers = get_judges(context, doc)
    assert min <= len(containers) <= max, (min, len(containers), max)

    for judge_container in containers:
        name, position = get_name_pos(context, judge_container, default_position)
        person_proxy = context.make("Person")
        person_proxy.id = context.make_id(name)
        person_proxy.add("sourceUrl", url)
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


def crawl(context: Context):
    crawl_judges(context, context.data_url, "Chief Justice", 1, 1)

    appeal_court_url = urljoin(context.data_url, "court-of-appeal")
    crawl_judges(context, appeal_court_url, "Justice of the Court of Appeal", 3, 10)

    grand_court_url = urljoin(context.data_url, "grand-court")
    crawl_judges(context, grand_court_url, "Judge of the Grand Court", 3, 10)
