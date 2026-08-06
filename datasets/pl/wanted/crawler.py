import time

from zavod.util import Element

from zavod import Context
from zavod import helpers as h

# Extract details using XPath based on the provided HTML structure
# required, key, xpath
ATTRIBUTES = [
    # The birth date is occasionally missing from an otherwise well-formed profile.
    # It is not required here so that a single such profile doesn't abort the crawl;
    # crawl_person skips the person instead (see below).
    (False, "birth_date", "//p[contains(text(), 'Data urodzenia:')]/strong/text()"),
    (True, "full_name", "//div[@class='head']/h2/text()"),
    (True, "gender", "//p[contains(text(), 'Płeć:')]/strong/text()"),
    (False, "middle_name", "//p[contains(text(), 'Drugie imię:')]/strong/text()"),
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


def extract_attributes(doc: Element, url: str) -> dict[str, str]:
    """Read the labelled fields off a profile page, without judging completeness.

    Fields the source gives as '-' come back as empty strings. Required fields are
    deliberately not enforced here: on a shell page every field is missing at once,
    and that has to be told apart from a real profile with a gap in it, so the
    `required` flag is checked by the caller once the page is known to have rendered.
    """
    info: dict[str, str] = {}
    for _, key, xpath in ATTRIBUTES:
        matches = h.xpath_strings(doc, xpath)
        text = ""
        if matches:
            assert len(matches) == 1, (key, url, matches)
            text = matches[0].strip()
        info[key] = "" if text == "-" else text
    return info


def fetch_person(context: Context, url: str) -> tuple[Element, dict[str, str]] | None:
    """Fetch a profile page, returning None if the source never rendered the record.

    The site intermittently serves a shell page: every field reads '-', the photo and
    physical-description blocks are absent, and only the name survives because it is
    drawn from the page title. Since responses are cached for a week, a single such
    response would keep breaking the crawl for days, so the cache entry is evicted and
    the request retried before the profile is given up on.
    """
    for attempt in range(4):
        if attempt:
            time.sleep(2**attempt)
        doc = context.fetch_html(url, cache_days=7)
        info = extract_attributes(doc, url)
        # A real profile always states at least one of these, so all three being
        # blank means the record body didn't render rather than being unknown.
        if any(info[key] for key in ("birth_date", "gender", "citizenship")):
            return doc, info
        context.clear_url(url)
        context.log.info("Profile did not render, evicted it from the cache", url=url)
    context.log.warning("Skipping profile the source failed to render", url=url)
    return None


def crawl_person(context: Context, url: str) -> None:
    fetched = fetch_person(context, url)
    if fetched is None:
        return
    doc, info = fetched

    for required, key, _ in ATTRIBUTES:
        if required:
            assert info[key], (key, url)

    if not info["birth_date"]:
        # The birth date is what distinguishes namesakes in the entity ID, so without
        # it we would risk merging distinct people into one entity.
        context.log.warning("Skipping person without a birth date", url=url)
        return

    person = context.make("Person")
    person.id = context.make_id(info.get("full_name"), info.get("birth_date"))
    person.add("sourceUrl", url)
    person.add("topics", "crime")
    person.add("topics", "wanted")

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

    if not person.has("citizenship"):
        person.add("country", "pl")

    crimes = h.xpath_elements(
        doc,
        "//p[contains(text(), 'Podstawy poszukiwań:')]/following-sibling::ul[1]//li",
    )
    if not crimes:
        context.log.warning("No crimes found for person", entity_id=person.id, url=url)
    for crime in crimes:
        person.add("notes", h.element_text(crime))

    context.audit_data(info)

    context.emit(person)


def crawl_index(context: Context, url: str) -> str | None:
    context.log.info(f"Crawling index page {url}")
    doc = context.fetch_html(url, absolute_links=True)
    # makes it easier to extract dedicated details page
    cells = h.xpath_strings(doc, "//li[.//a[contains(@href, '/pos/form/r')]]/a/@href")
    for cell in cells:
        crawl_person(context, cell)

    # On the last page, the next button will not have an <a>, so this will not match
    next_button_href = h.xpath_strings(
        doc, "//li/a/span[contains(text(), 'następna')]/parent::a/@href"
    )
    return next_button_href[0] if next_button_href else None


def crawl(context: Context) -> None:
    assert context.dataset.data is not None
    next_url: str | None = context.dataset.data.url
    # Use this construction instead of recursion because Python sets a recursion limit
    while next_url:
        next_url = crawl_index(context, next_url)
