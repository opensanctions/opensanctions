from normality import collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.util import Element

# Personal particulars are published as "Label : Value" text nodes separated by
# <br/>, not as a table, and the labels drift between profiles. Anything not
# mapped below is kept as a note rather than dropped.
BIRTH_YEAR = "Year of Birth"
BIRTH_PLACE = "Place of Birth"
SEX = "Sex"
NATIONALITY = "Nationality"
OCCUPATION = "Occupation"

# Values published as unknown carry no information.
UNKNOWN_VALUES = {"unknown", "n/a", "-"}

# Every identity card and passport number on this source is partially masked
# (e.g. "P868xxx(x)", "BB24XXXXX"). That holds for all of them across every
# profile, so none are emitted as idNumber or passportNumber; they are kept as
# notes so the record still shows which documents the ICAC listed.
IDENTIFIER_HINTS = ("passport", "hkic", " id ", "id no", "identity", "permit")


def is_identifier_label(label: str) -> bool:
    padded = f" {label.lower()} "
    return any(hint in padded for hint in IDENTIFIER_HINTS)


def parse_detail_table(doc: Element) -> dict[str, str]:
    """Read the Name / Alias / Charge(s) table at the top of a profile page."""
    values: dict[str, str] = {}
    for row in doc.findall('.//div[@class="wpInfoDetail"]//tr'):
        label_el = row.find("./th")
        value_el = row.find("./td")
        if label_el is None or value_el is None:
            continue
        label = collapse_spaces(h.element_text(label_el))
        value = collapse_spaces(h.element_text(value_el))
        if not label or not value:
            continue
        values[label.rstrip(" :")] = value
    return values


def parse_bio(doc: Element) -> dict[str, str]:
    """Read the "Personal Particular" block.

    The fields are separated by <br/>, so the direct text nodes are one field
    each. element_text() would squash the line breaks away.
    """
    values: dict[str, str] = {}
    for line in h.xpath_strings(doc, './/div[@class="wanted-bio"]/text()'):
        raw_label, sep, raw_value = line.partition(":")
        if not sep:
            continue
        label = collapse_spaces(raw_label)
        value = collapse_spaces(raw_value)
        if not label or not value or value.lower() in UNKNOWN_VALUES:
            continue
        values[label] = value
    return values


def crawl_person(context: Context, url: str, last_name: str | None) -> None:
    doc = context.fetch_html(url, cache_days=1)
    details = parse_detail_table(doc)
    bio = parse_bio(doc)

    name = details.get("Name")
    if name is None:
        context.log.warning("No name on profile page", url=url)
        return

    person = context.make("Person")
    person.id = context.make_id(name, bio.get(BIRTH_YEAR), url)
    person.add("name", name)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("country", "hk")
    person.add("sourceUrl", url)

    # The index page carries the family name separately, which is the only
    # reliable way to split these names.
    person.add("lastName", last_name)

    for raw_alias in details.get("Alias", "").split("/"):
        alias = collapse_spaces(raw_alias)
        if alias is not None:
            prop = "alias" if " " in alias else "weakAlias"
            person.add(prop, alias)

    charges = details.get("Charge(s)")
    if charges is not None:
        person.add("notes", f"Charge(s): {charges}")

    birth_year = bio.pop(BIRTH_YEAR, None)
    if birth_year is not None:
        h.apply_date(person, "birthDate", birth_year)

    person.add("birthPlace", bio.pop(BIRTH_PLACE, None))
    person.add("gender", bio.pop(SEX, None))
    person.add("position", bio.pop(OCCUPATION, None))

    # Some entries carry two nationalities separated by a slash, and some carry
    # a trailing slash with nothing after it.
    for raw_nationality in bio.pop(NATIONALITY, "").split("/"):
        person.add("nationality", collapse_spaces(raw_nationality))

    # Physical description, dialects spoken, remarks and the masked document
    # numbers have no property of their own but are worth keeping.
    for label, value in bio.items():
        if is_identifier_label(label):
            person.add("notes", f"{label} (masked at source): {value}")
        else:
            person.add("notes", f"{label}: {value}")

    case_brief = doc.find('.//div[@class="caseBrief"]')
    if case_brief is not None:
        person.add("notes", h.element_text(case_brief))

    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)

    seen: set[str] = set()
    for link in h.xpath_elements(doc, './/a[contains(@href, "index_id_")]'):
        url = link.get("href")
        # People recently added appear both in the "Newly Wanted Person(s)"
        # block and again in the main list below it.
        if url is None or url in seen:
            continue
        seen.add(url)

        family_name = link.find('.//span[@class="hf_family_name"]')
        last_name = None
        if family_name is not None:
            last_name = collapse_spaces(h.element_text(family_name))

        crawl_person(context, url, last_name)

    assert len(seen) > 30, "Suspiciously few wanted persons on the index page"
