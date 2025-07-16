from typing import cast
from banal import ensure_list
from lxml.etree import _Element

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_item(li_tag: _Element, context: Context) -> None:
    name = li_tag.findtext(".//a/b")
    names = [] if name is None else [name]
    li_link = li_tag.find(".//a")
    if li_link is None:
        return
    try:
        description = li_tag.xpath(".//br/following-sibling::text()")[0]
    except IndexError:
        description = None

    if not names:
        long_name = li_link.text_content()
        long_name = long_name.replace("SEC Advisory on", "").strip()
        res = context.lookup("names", long_name)
        if not res:
            name = long_name
            description = None
            context.log.warning("No lookup for name: %s" % long_name)
            import json

            print("- match: %s" % json.dumps(long_name))
            print("  name: %s" % json.dumps(long_name))
            return

        if res.ignore is True:
            return

        if res.name is not None:
            names = [str(n) for n in ensure_list(res.name)]
        else:
            names = [long_name]
        description = cast("str", res.description) or long_name

    if any(context.lookup("urgent_skip", name) for name in names):
        context.log.info("Skipping %s" % name)
        return

    source_url = li_link.get("href")
    date = li_tag.findtext(".//*[@class='myDate']").strip()

    entity = context.make("LegalEntity")
    entity.id = context.make_id(source_url)
    entity.add("name", names)
    entity.add("topics", "reg.warn")
    entity.add("notes", description)
    entity.add("sourceUrl", source_url)
    h.apply_date(entity, "createdAt", date)
    context.emit(entity)


def crawl(context: Context):
    list_xpath = ".//*[@class='accordion-content']/ul/li"
    doc = fetch_html(context, context.data_url, list_xpath)
    for item in doc.findall(list_xpath):
        crawl_item(item, context)
