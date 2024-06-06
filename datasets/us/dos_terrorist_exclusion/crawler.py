from zavod import Context, helpers as h
import requests
from typing import cast
from lxml import html

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
}


def crawl_item(raw_name: str, context: Context):

    entity = context.make("LegalEntity")

    # If it does not have parenthesis, that it doesn't have aliases
    if "(" not in raw_name:
        name = raw_name
        aliases = []

    else:
        name = h.remove_bracketed(raw_name)

        res = context.lookup("aliases", raw_name)
        if res:
            aliases = cast("List[str]", res.names)
        else:
            context.log.warning(f"Could not find aliases for {raw_name}")
            aliases = []

    entity.id = context.make_id(name)
    entity.add("name", name)

    for alias in aliases:
        entity.add("alias", alias)

    entity.add("topics", "crime.terror")

    sanction = h.make_sanction(context, entity)

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):

    response = requests.get(context.data_url, headers=HEADERS)

    doc = html.fromstring(response.text)

    # Find the title of the list by the text, then find the next sibling (which is the list), then get all the list items texts
    xpath = ".//*[contains(text(), 'Terrorist Exclusion List Designees (alphabetical listing)')]/../following-sibling::*[1]/li/text()"

    for item in doc.xpath(xpath):
        crawl_item(item, context)
