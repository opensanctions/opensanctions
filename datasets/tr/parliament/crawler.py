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
    }
]


def index_validator(doc):
    return len(doc.xpath('//ul[contains(@class, "tbmm-list-ul")]')) > 0


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
        context.emit(entity, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    doc = fetch_html(
        context,
        context.data_url,
        index_validator,
        actions=UNBLOCK_ACTIONS,
        javascript=True,
        cache_days=1,
    )
    doc.make_links_absolute(context.data_url)

    for item in doc.xpath('//li[contains(@class, "tbmm-list-item")]'):
        crawl_item(context, item)
