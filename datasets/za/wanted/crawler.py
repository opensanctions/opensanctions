from zavod import Context
from lxml import html
from urllib.parse import urlparse, parse_qs
from typing import Dict
import re

from zavod import helpers as h

REGEX_PATTERN = re.compile("(.+)\((.+)\)(.+)")


def parse_cell(cell: html.HtmlElement):
    result = {}
    text = cell.text_content()
    match = REGEX_PATTERN.match(text.strip())
    if match:
        name, crime, status = match.groups()
        last_name, first_name = name.split(",", maxsplit=1)
        result["first_name"] = first_name.strip()
        result["last_name"] = last_name.strip()
        result["crime"] = crime.strip()
        result["status"] = status.strip()
        details_id = cell.xpath(".//a/@href")[0]
        source_url = (
            f'https://www.saps.gov.za/crimestop/wanted/{details_id}'
        )
        result["source_url"] = source_url
    return result


def crawl_cell(context: Context, cell: Dict[str, str]):
    data = parse_cell(cell)

    person = context.make("Person")
    
    # each wanted person has a dedicated details page 
    # which appears to be a unique identifier
    id = parse_qs(urlparse(data["source_url"]).query)["bid"][0]
    person.id = context.make_slug(id)
    breakpoint()

    h.apply_name(
        person,
        first_name=data.get("first_name"),
        last_name=data.get("last_name"),
    )

    person.add("sourceUrl", data.get("source_url"))
    person.add("notes", f"{data.get('status')} - {data.get('crime')}")

    person.add("topics", "crime")
    context.emit(person, target=True)


def crawl(context):
    doc = context.fetch_html(context.dataset.data.url, cache_days=1)
    cells = doc.xpath("//td[.//a[starts-with(@href, 'detail.php')]]")

    for cell in cells:
        crawl_cell(context, cell)
