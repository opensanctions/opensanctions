from zavod import Context
from lxml import html
from urllib.parse import urlparse, parse_qs
import re

from zavod import helpers as h

REGEX_PATTERN = re.compile("(.+)\((.+)\)(.+)")

def crawl_person(context: Context, cell: html.HtmlElement):
    source_url = cell.xpath(".//a/@href")[0]
    match = REGEX_PATTERN.match(cell.text_content())

    if not match:
        context.log.warning("Regex did not match data for person %s" % source_url)
        return

    name, crime, status = map(str.strip, match.groups())
    last_name, first_name = map(str.strip, name.split(",", maxsplit=1))

    # either first or last name is considered a bare minimum to emit a person entity
    unknown_spellings = ["Unknown", "Uknown"]
    if first_name in unknown_spellings and last_name in unknown_spellings:
        return

    person = context.make("Person")
    
    # each wanted person has a dedicated details page 
    # which appears to be a unique identifier
    id = parse_qs(urlparse(source_url).query)["bid"][0]
    person.id = context.make_slug(id)

    h.apply_name(
        person,
        first_name=first_name,
        last_name=last_name,
    )

    person.add("sourceUrl", source_url)
    person.add("notes", f"{status} - {crime}")

    person.add("topics", "crime")
    context.emit(person, target=True)


def crawl(context):
    doc = context.fetch_html(context.dataset.data.url, cache_days=1)
    # makes it easier to extract dedicated details page 
    doc.make_links_absolute(context.dataset.data.url)
    cells = doc.xpath("//td[.//a[contains(@href, 'detail.php')]]")

    for cell in cells:
        crawl_person(context, cell)
