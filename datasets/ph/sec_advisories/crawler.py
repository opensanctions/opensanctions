from typing import cast

from banal import ensure_list
from zavod.extract.zyte_api import fetch_html
from zavod.util import Element

from zavod import Context
from zavod import helpers as h


def crawl_item(li_tag: Element, context: Context) -> None:
    name = li_tag.findtext(".//a/b")
    names = [] if name is None else [name]
    li_link = li_tag.find(".//a")
    if li_link is None:
        return
    texts = h.xpath_strings(li_tag, ".//br/following-sibling::text()")
    description: str | None = texts[0] if texts else None

    if not names:
        long_name = h.element_text(li_link)
        long_name = long_name.replace("SEC Advisory on", "").strip()
        res = context.lookup("names", long_name, warn_unmatched=True)
        if not res:
            name = long_name
            description = None
            import json

            print(f"- match: {json.dumps(long_name)}")
            print(f"  name: {json.dumps(long_name)}")
            return

        if res.ignore is True:
            return

        if res.name is not None:
            names = [str(n) for n in ensure_list(res.name)]
        else:
            names = [long_name]
        description = cast("str", res.description) or long_name

    if any(context.lookup("urgent_skip", name) for name in names):
        context.log.info(f"Skipping {name}")
        return

    source_url = li_link.get("href")
    date_text = li_tag.findtext(".//*[@class='myDate']")
    assert date_text is not None, li_tag
    date = date_text.strip()

    entity = context.make("LegalEntity")
    entity.id = context.make_id(source_url)
    entity.add("name", names)
    entity.add("topics", "reg.warn")
    entity.add("notes", description)
    entity.add("sourceUrl", source_url)
    h.apply_date(entity, "createdAt", date)
    context.emit(entity)


UNBLOCK_ACTIONS = [
    {
        "action": "waitForNavigation",
        "waitUntil": "networkidle0",
        "timeout": 31,
        "onError": "return",
    },
    {
        "action": "waitForSelector",
        "selector": {
            "type": "xpath",
            "value": ".//*[@class='accordion-content']/ul/li",
            "state": "visible",
        },
        "timeout": 15,
        "onError": "return",
    },
]


def crawl(context: Context) -> None:
    list_xpath = ".//*[@class='accordion-content']/ul/li"
    doc = fetch_html(context, context.data_url, list_xpath, actions=UNBLOCK_ACTIONS)
    for item in doc.findall(list_xpath):
        crawl_item(item, context)
