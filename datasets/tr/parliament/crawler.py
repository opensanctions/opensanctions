###############################################################################
#
# Common issues
#
# - Assertion failed for value 60: <Assertion entity_count gte 590 filter: schema=Person>
#
#     Vastly fewer entities than expected. All entities are loaded from a single page
#     and it's not clear why we don't get a complete response. It could be because
#     the page loads slowly and the action timeout is reached before the page is fully
#     loaded.
#
#     Usually this works on the next run.
#
###


from lxml import etree
import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.extract.zyte_api import fetch_html

UNBLOCK_ACTIONS = [
    {
        "action": "waitForNavigation",
        "waitUntil": "networkidle0",
        "timeout": 31,
        "onError": "return",
    },
    {
        "action": "waitForTimeout",
        "timeout": 15,
        "onError": "return",
    },
]


def crawl_birth_year_place(context: Context, url: str) -> tuple[int | None, str | None]:
    doc = fetch_html(
        context,
        url,
        '//*[@id="milletvekili-detay-holder-desktop"]',
        actions=UNBLOCK_ACTIONS,
        javascript=True,
        absolute_links=True,
    )

    xpath = '//*[@id="milletvekili-detay-holder-desktop"]//div[contains(@class, "profile-ozgecmis-div")]/span//*[1]/text()'

    birth_string = h.xpath_strings(doc, xpath)
    match = re.match(r"^([\w\s/]+?)\s*–\s*(\d{4})", birth_string[0])
    year = int(match.group(2)) if match else None
    birth_place = match.group(1).strip() if match else None

    return year, birth_place


def crawl_item(context: Context, item: etree):
    anchor = item.find(".//a")
    if anchor is None:
        return
    deputy_url = anchor.get("href")
    name = anchor.text_content().strip()
    party_els = item.xpath('.//div[contains(@class, "text-right")]')
    assert len(party_els) == 1
    party = party_els[0].text_content().strip()

    entity = context.make("Person")
    entity.id = context.make_slug(name, party)
    entity.add("name", name)
    entity.add("sourceUrl", deputy_url)
    entity.add("political", party)

    birth_year, birth_place = crawl_birth_year_place(context, deputy_url)
    entity.add("birthDate", birth_year)
    entity.add("birthPlace", birth_place)

    position = h.make_position(
        context, "Member of the Grand National Assembly", country="tr"
    )
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        categorisation=categorisation,
    )

    ### creating a temp entity to test the id rekey ###
    entity_temp = context.make("Person")
    if birth_year is not None:
        entity_temp.id = context.make_id(name, str(birth_year), birth_place)
    entity_temp.add("name", name)
    entity_temp.add("birthDate", birth_year)
    entity_temp.add("birthPlace", birth_place)
    entity_temp.add("political", party)
    ### === ### === ### === ### === ### === ### === ###

    if occupancy:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)
        context.emit(entity_temp, external=True)  # emit temp entity to test the rekey


def crawl(context: Context):
    items_xpath = '//li[contains(@class, "tbmm-list-item")]'
    doc = fetch_html(
        context,
        context.data_url,
        items_xpath,
        actions=UNBLOCK_ACTIONS,
        javascript=True,
        absolute_links=True,
    )

    for item in doc.xpath(items_xpath):
        crawl_item(context, item)
