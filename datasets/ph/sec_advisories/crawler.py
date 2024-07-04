# import json
from typing import cast
from lxml.etree import _Element

from zavod import Context
from zavod import helpers as h


def crawl_item(li_tag: _Element, context: Context) -> None:
    name = li_tag.findtext(".//a/b")
    li_link = li_tag.find(".//a")
    if li_link is None:
        return
    try:
        description = li_tag.xpath(".//br/following-sibling::text()")[0]
    except IndexError:
        description = None

    if name is None:
        long_name = li_link.text_content()
        long_name = long_name.replace('SEC Advisory on', '').strip()
        res = context.lookup("names", long_name)
        if not res:
            name = long_name
            description = None
            context.log.warning(
                "No lookup for name: %s" % long_name
            )
            # print("- %s" % json.dumps(long_name))
            return

        if res.ignore is True:
            return

        if res.name is not None:
            name = cast("str", res.name)
        description = cast("str", res.description)

    source_url = li_link.get("href")
    date = li_tag.findtext(".//*[@class='myDate']")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(source_url)
    entity.add("name", name)
    entity.add("topics", "reg.warn")

    sanction = h.make_sanction(context, entity)
    sanction.add("description", description)
    sanction.add("sourceUrl", source_url)
    sanction.add("date", h.parse_date(date, formats=["%d %M %Y"]))

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)
    for item in response.findall(".//*[@class='accordion-content']/ul/li"):
        crawl_item(item, context)
