from lxml import html
from zavod import Context
from typing import Dict, List


def parse_li_element(li_element: html.HtmlElement) -> Dict[str, str]:
    """Parse a single <li> element to extract the name and link."""
    link_element = li_element.xpath(".//a")
    name = li_element.text_content().strip()
    link = (
        f"https://en.wikipedia.org{link_element[0].get('href')}"
        if link_element
        else None
    )

    return {"Name": name, "Link": link}


def crawl_item(context: Context, row: Dict[str, str]):
    name = row.pop("Name")
    link = row.pop("Link", None)

    entity = context.make("Organization")
    entity.id = context.make_id(name, link)
    entity.add("name", name)
    entity.add("topics", "sanction")
    entity.add("topics", "crime.terror")

    if link:
        entity.add("sourceUrl", link)

    context.emit(entity, target=True)
    context.audit_data(row)


def parse_list(doc: html.HtmlElement) -> List[Dict[str, str]]:
    """Parse the HTML document to extract the list of designated organizations."""
    # Find all <li> elements within the specified <div>
    li_elements = doc.xpath(
        '//div[@class="div-col" and @style="column-width: 30em;"]//li'
    )
    return [parse_li_element(li) for li in li_elements]


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    rows = parse_list(doc)

    if not rows:
        context.log.warn("No data found in the document.")
        return

    for row in rows:
        crawl_item(context, row)
