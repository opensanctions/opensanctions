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

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html

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

    if occupancy:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    items_xpath = '//li[contains(@class, "tbmm-list-item")]'
    doc = fetch_html(
        context,
        context.data_url,
        items_xpath,
        actions=UNBLOCK_ACTIONS,
        javascript=True,
    )
    doc.make_links_absolute(context.data_url)

    for item in doc.xpath(items_xpath):
        crawl_item(context, item)
