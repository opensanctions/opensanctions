from zavod import Context
from zavod import helpers as h

# Extract details using XPath based on the provided HTML structure
# required, key, xpath
ATTRIBUTES = [
    (True, "birth_date", "//p[contains(text(), 'Data urodzenia:')]/strong/text()"),
    (True, "full_name", "//div[@class='head']/h2/text()"),
    (True, "gender", "//p[contains(text(), 'Płeć:')]/strong/text()"),
    (True, "middle_name", "//p[contains(text(), 'Data urodzenia:')]/strong/text()"),
    (False, "alias", "//p[contains(text(), 'Pseudonim:')]/strong/text()"),
    (False, "birth_place", "//p[contains(text(), 'Miejsce urodzenia:')]/strong/text()"),
    (False, "citizenship", "//p[contains(text(), 'Obywatelstwo:')]/strong/text()"),
    (False, "eye_color", "//p[contains(text(), 'Kolor oczu:')]/strong/text()"),
    (False, "father_name", "//p[contains(text(), 'Imię ojca:')]/strong/text()"),
    (False, "height", "//p[contains(text(), 'Wzrost:')]/strong/text()"),
    (
        False,
        "mother_maiden_name",
        "//p[contains(text(), 'Nazwisko panieńskie matki:')]/strong/text()",
    ),
    (False, "mother_name", "//p[contains(text(), 'Imię matki:')]/strong/text()"),
    (False, "hair_color", "//li[contains(text(), 'WŁOSY:')]/text()"),
]


def crawl_person(context: Context, url: str):
    doc = context.fetch_html(url, cache_days=7)

    info = dict()
    for required, key, xpath in ATTRIBUTES:
        matches = doc.xpath(xpath)
        text = ""
        if required or matches:
            assert len(matches) == 1, (key, url, matches)
            text = matches[0].strip()
        text = "" if text == "-" else text
        if required:
            assert text, (key, url)
        info[key] = text

    person = context.make("Person")
    person.id = context.make_id(info.get("full_name"), info.get("birth_date"))
    person.add("sourceUrl", url)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("country", "pl")

    h.apply_name(
        person, full=info.pop("full_name"), middle_name=info.pop("middle_name")
    )
    h.apply_date(person, "birthDate", info.pop("birth_date"))
    person.add("birthPlace", info.pop("birth_place", None))
    person.add("gender", info.pop("gender"))
    person.add("alias", info.pop("alias", None))
    person.add("fatherName", info.pop("father_name", None))
    person.add("motherName", info.pop("mother_name", None))
    person.add("motherName", info.pop("mother_maiden_name", None))
    person.add("height", info.pop("height", None))
    person.add("eyeColor", info.pop("eye_color", None))
    person.add("hairColor", info.pop("hair_color", "").replace("WŁOSY:", ""))

    citizenship = info.pop("citizenship", None)
    citizenship_original_value = citizenship
    if citizenship and "POLSKA" in citizenship:
        person.add("citizenship", "pl", original_value=citizenship_original_value)
        citizenship = citizenship.replace("POLSKA", "").strip()
    person.add(
        "citizenship",
        h.multi_split(citizenship, ["(", ")"]),
        original_value=citizenship_original_value,
    )

    crimes = doc.xpath(
        "//p[contains(text(), 'Podstawy poszukiwań:')]/following-sibling::ul//a/text()"
    )
    if not crimes:
        context.log.warn("No crimes found for person", entity_id=person.id, url=url)
    for crime in crimes:
        person.add("notes", f"Wanted for: {crime}")

    context.audit_data(info)

    context.emit(person)


def crawl_index(context, url) -> str | None:
    context.log.info(f"Crawling index page {url}")
    doc = context.fetch_html(url)
    # makes it easier to extract dedicated details page
    doc.make_links_absolute(context.dataset.data.url)
    cells = doc.xpath("//li[.//a[contains(@href, '/pos/form/r')]]/a/@href")
    for cell in cells:
        crawl_person(context, cell)

    # On the last page, the next button will not have an <a>, so this will not match
    next_button_href = doc.xpath(
        "//li/a/span[contains(text(), 'następna')]/parent::a/@href"
    )
    return next_button_href[0] if next_button_href else None


def crawl(context):
    next_url = context.dataset.data.url
    # Use this construction instead of recursion because Python sets a recursion limit
    while next_url:
        next_url = crawl_index(context, next_url)
