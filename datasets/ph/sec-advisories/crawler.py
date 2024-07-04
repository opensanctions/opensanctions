from typing import cast
from lxml.etree import _Element

from zavod import Context, helpers as h


def crawl_item(li_tag: _Element, context: Context):

    name = li_tag.findtext(".//a/b")
    try:
        description = li_tag.xpath(".//br/following-sibling::text()")[0]
    except IndexError:
        description = None

    if name is None:
        res = context.lookup("names", li_tag.find(".//a").text_content())

        if not res:
            name = li_tag.find(".//a").text_content()
            description = None
            context.log.warning(
                "Can't find the name of the entity "
                + li_tag.find(".//a").text_content()
            )
            return

        names = cast("List[str]", res.name)
        description = cast("str", res.description)

    names = [name] if not isinstance(name, list) else name

    source_url = li_tag.find(".//a").get("href")
    date = li_tag.findtext(".//*[@class='myDate']")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(names)
    for name in names:
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
