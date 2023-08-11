from urllib.parse import urljoin
from lxml.etree import _Element
from normality import slugify

from zavod import Context

FIELD_NAMES = ("basic", "description", "additional")


def crawl_person(context: Context, item: _Element, url: str) -> None:
    name = item.find('.//a[@itemprop="url"]')
    if name is None or name.text is None:
        context.log.error("Cannot find name row", url=url)
        return

    # Person create
    person = context.make("Person")
    person.add("topics", "crime")
    person.add("name", name.text.strip())
    person.id = context.make_slug(name.text.strip())

    # Person page
    person_url = urljoin(url, name.get("href"))
    person.add("sourceUrl", person_url)
    doc = context.fetch_html(person_url, cache_days=7)

    # Article
    article = doc.find('.//div[@itemprop="articleBody"]').find(".//p")
    person.add("notes", article.text.strip())

    # Fields
    for field_name in FIELD_NAMES:
        column = doc.find(
            f'.//div[@class="span4 most-wanted-customfields most-wanted-{field_name}"]'
        )
        labels = column.findall('./span[@class="field-label "]')
        values = column.findall('./span[@class="field-value "]')

        for label, value in dict(zip(labels, values)).items():
            label_text = slugify(label.text)
            if label_text == "sex":
                person.add("gender", value.text.strip())
            elif label_text == "ethnic-appearance":
                person.add("ethnicity", value.text.strip())
            elif label_text == "additional-information":
                person.add("notes", [article.text.strip(), value.text.strip()])

    context.emit(person, target=True)


def crawl(context: Context):
    url = context.data_url
    doc = context.fetch_html(url)
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
