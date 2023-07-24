from urllib.parse import urljoin

from zavod import Context
from opensanctions import helpers as h

FIELDS = {
    "Sex": "gender",
    "Alias": "alias",
    "Date of birth": "birthDate",
    "Crime": "notes",
    "Nationality": "nationality",
}


def parse_date(text):
    return h.parse_date(text, ["%b %d, %Y", "%B %d, %Y"])


def crawl(context: Context):
    base_url = context.data_url
    doc = context.fetch_html(base_url)
    for link in doc.findall(".//span[@class='field-content']"):
        url = link.text
        if url is None or not url.startswith("/"):
            continue
        if url.startswith("/el/"):
            continue
        url = urljoin(base_url, url)
        if "legal-notice" in url:
            continue
        crawl_person(context, link.text, url)


def crawl_person(context: Context, person_id: str, url: str):
    """read and parse every person-page"""
    doc = context.fetch_html(url)
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("topics", "crime")
    person.add("sourceUrl", url)

    title = doc.findtext(".//title")
    name, _ = title.split("|")
    person.add("name", name.strip())

    for field in doc.findall(".//div[@class='wanted_top_right']/div"):
        label = field.findtext(".//div[@class='field-label']")
        if label is None:
            continue
        prop = FIELDS.get(label)
        if prop is None:
            # context.log.info("Unknown field", label=label)
            continue
        for item in field.findall(".//div[@class='field__item']"):
            value = item.text
            item_time = item.find(".//time")
            if item_time is not None:
                value = item_time.get("datetime").split("T")[0]
            person.add(prop, value)
            # print(label, value)
        # context.inspect(field)

    # print(person.to_dict())
    context.emit(person, target=True)
