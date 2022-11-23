from urllib import parse
import re

from lxml.html import HtmlElement
from opensanctions.core import Context

NCA_URL = "https://www.nationalcrimeagency.gov.uk"

FIELD_NAMES = (
    "basic",
    "description",
    "additional"
)

def fix_label(label: str):
    text = re.sub(r'(?<=[a-z])(?=[A-Z])|[^a-zA-Z]', ' ', label).strip().replace(' ', '-')
    return ''.join(text.lower())


def crawl_person(context: Context, item: HtmlElement, url: str) -> None:
    name = item.find('.//a[@itemprop="url"]')
    if name is None:
        context.log.error("Cannot find name row", url=url)
        return

    # Person page
    person_url = parse.urljoin(url, name.get("href"))
    person.add("sourceUrl", person_url)
    person.add("topics", "crime")
    doc = context.fetch_html(person_url, cache_days=7)
    
    # Person
    person = context.make("Person")
    person.add("name", name.text.strip())
    person.id = context.make_slug(name.text.strip())

    # Article
    article = doc.find('.//div[@itemprop="articleBody"]').find('.//p')
    person.add("notes", article.text.strip())
    
    # Fields
    for field_name in FIELD_NAMES:
        column = doc.find(f'.//div[@class="span4 most-wanted-customfields most-wanted-{field_name}"]')
        labels = column.findall('./span[@class="field-label "]')
        values = column.findall('./span[@class="field-value "]')

        for label, value in dict(zip(labels, values)).items():
            if fix_label(label.text) == "sex":
                person.add("gender", value.text.strip())
            elif fix_label(label.text) == "ethnic-appearance":
                person.add("ethnicity", value.text.strip())
            elif fix_label(label.text) == "additional-information":
                person.add("notes", [article.text.strip(), value.text.strip()])

    context.emit(person, target=True)


def crawl_page(context: Context, url: str) -> None:
    doc = context.fetch_html(url, cache_days=7)
    mw_grid = doc.find('.//div[@class="blog most-wanted-grid"]')
    if mw_grid is None:
        context.log.debug("Cannot find fact detailed list", url=url)
        return

    items_rows = mw_grid.findall("./div")
    if items_rows is None:
        context.log.error("Cannot find any rows", url=url)
        return

    for item_row in items_rows:
        for item in item_row.findall("./div"):
            crawl_person(context, item, url)


def crawl(context: Context):
    url = parse.urljoin(NCA_URL, "/most-wanted")
    crawl_page(context, url)
