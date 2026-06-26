from urllib.parse import urljoin

from zavod import Context

FIELDS = {
    "Sex": "gender",
    "Alias": "alias",
    "Date of birth": "birthDate",
    "Crime": "notes",
    "Nationality": "nationality",
    "Eye colour": "eyeColor",
    "Hair color": "hairColor",
    "Approximate height": "height",
    "Ethnic origin": "ethnicity",
    "Identifiers": "appearance",
    "State of case": "notes",
}


def crawl(context: Context) -> None:
    base_url = context.data_url
    doc = context.fetch_html(base_url)
    for link in doc.findall(".//span[@class='field-content']"):
        person_id = link.text
        if person_id is None or not person_id.startswith("/"):
            continue
        if person_id.startswith("/el/"):
            continue
        url = urljoin(base_url, person_id)
        if "legal-notice" in url:
            continue
        crawl_person(context, person_id=person_id, url=url)


def crawl_person(context: Context, *, person_id: str, url: str) -> None:
    """read and parse every person-page"""
    doc = context.fetch_html(url)
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("sourceUrl", url)

    title = doc.findtext(".//title")
    assert title is not None
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
                datetime_attr = item_time.get("datetime")
                assert datetime_attr is not None
                value = datetime_attr.split("T")[0]
            person.add(prop, value)

            # print(label, value)
        # context.inspect(field)

    # print(person.to_dict())
    context.emit(person)
