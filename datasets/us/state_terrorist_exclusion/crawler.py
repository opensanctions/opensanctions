from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_item(raw_name: str, context: Context):

    entity = context.make("LegalEntity")

    names = h.multi_split(
        raw_name, ["; a.k.a.", "(a.k.a.", "(f.k.a.", "; f.k.a.", ", a.k.a."]
    )

    entity.id = context.make_id(names)

    for name in names:
        # If there is some name with a ) at the end without a (, we remove it
        name = name[:-1] if "(" not in name and name[-1] == ")" else name
        entity.add("name", name)

    entity.add("topics", "sanction")
    entity.add(
        "program",
        "Section 411 of the USA PATRIOT ACT of 2001 (8 U.S.C. § 1182) Terrorist Exclusion List (TEL) ",
    )

    context.emit(entity, target=True)


def crawl(context: Context):

    # Find the title of the list by the text, then find the next sibling
    # (which is the list), then get all the list items texts
    list_xpath = ".//*[contains(text(), 'Terrorist Exclusion List Designees (alphabetical listing)')]/following-sibling::*[1]/li/text()"

    actions = [
        {
            "action": "waitForSelector",
            "selector": {
                "type": "xpath",
                "value": list_xpath,
            },
            "timeout": 15,
        },
    ]
    doc = fetch_html(context, context.data_url, list_xpath, actions)

    for item in doc.xpath(list_xpath):
        crawl_item(item, context)
