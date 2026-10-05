import re
from urllib.parse import urljoin

from lxml.etree import _Element as Element

from zavod import Context
from zavod import helpers as h
from zavod.stateful.review import assert_all_accepted

# Charge-sheet accused numbers appended to names and aliases, e.g. "Masud K A (A-5)",
# "Shaik Mohammed Imran Akram, (A-8)", "Niu Niu WA-1", "@ Rafikl Mia (A-4)."
ACCUSED_NO = re.compile(r"[\s,]*\(?\bW?A-\d+\)?\.?\s*$")
# Aliases are separated by "@", occasionally by "alias".
ALIAS_SPLIT = re.compile(r"@|\balias\b", re.IGNORECASE)
# "Son of" markers, e.g. "S/o Narendar Singh" in the alias field.
SON_OF = re.compile(r"^s/o\s+", re.IGNORECASE)
# Several addresses in one field, e.g. "Vill. Mangror, Punjab and Present Address: ...",
# "716, Sri Ganga Nagar, Rajasthan Address in UK Number 3, ...".
ADDRESS_SPLIT = re.compile(
    r"(?:\band\s+)?\b(?:(?:present|permanent|work)\s+address\s*:|address in \w+\b"
    r"|presently r/o\b)",
    re.IGNORECASE,
)
RESIDENT_OF = re.compile(r"^(r/o|resident of)(\s+|$)", re.IGNORECASE)
# Approximate ages without a reference date, e.g. "31 years", "About 55 Yrs".
AGE = re.compile(r"^(about[\s-]*)?\d+ (years|yrs)$", re.IGNORECASE)


def clean_part(value: str) -> str:
    return ACCUSED_NO.sub("", value).strip(" ,.")


def crawl_case_title(context: Context, url: str) -> str:
    doc = context.fetch_html(url, cache_days=7)
    return h.xpath_string(
        doc,
        "//div[contains(@class, 'view-nia-cases')]//b[text()='Case Title']"
        "/following-sibling::p[1]/text()",
    ).strip()


def crawl_person(context: Context, card: Element, page_url: str) -> None:
    fields: dict[str, Element] = {}
    for row in h.xpath_elements(card, ".//div[@class='modal-detail-sec']/div"):
        label = h.element_text(h.xpath_element(row, "./div[contains(@class, 'lab')]"))
        fields[label.rstrip(" :")] = h.xpath_element(
            row, "./div[contains(@class, 'val')]"
        )

    name = h.element_text(fields.pop("Name"))
    aliases = h.element_text(fields.pop("Aliases"))
    wanted_in = fields.pop("Wanted in")
    wanted_in_text = h.element_text(wanted_in)

    person = context.make("Person")
    person.id = context.make_id(name, aliases, wanted_in_text)
    person.add("topics", "wanted")

    names = h.Names(name=clean_part(name))
    for part in ALIAS_SPLIT.split(aliases):
        part = clean_part(part)
        if SON_OF.match(part):
            person.add("fatherName", SON_OF.sub("", part))
        elif part != "":
            names.add("alias", part)
    h.apply_reviewed_names(context, person, original=names, llm_cleaning=True)

    for address in ADDRESS_SPLIT.split(h.element_text(fields.pop("Address"))):
        address = RESIDENT_OF.sub("", clean_part(address))
        h.copy_address(person, h.make_address(context, full=address))

    status = h.element_text(fields.pop("Accused Status"))
    if status != "":
        person.add(
            "status",
            context.lookup_value("accused_status", status, warn_unmatched=True),
            original_value=status,
        )

    # Ages are given without the date they were recorded, so no birth date
    # can be derived from them.
    age = h.element_text(fields.pop("Age/DOB (Approx)"))
    if age != "" and not AGE.match(age):
        context.log.warning("Unexpected age value", name=name, age=age)

    # Parentage and Organization are empty for every listed person so far.
    for label in ("Parentage", "Organization"):
        value = h.element_text(fields.pop(label))
        if value != "":
            context.log.warning(f"Unhandled {label} value", name=name, value=value)
    if fields:
        context.log.warning("Unhandled fields", name=name, fields=list(fields))

    case_links = h.xpath_elements(wanted_in, ".//a[@href!='']")
    for link in case_links:
        case_no = h.element_text(link)
        case_url = urljoin(page_url, link.get("href"))
        case_title = crawl_case_title(context, case_url)
        person.add("notes", f"Wanted in NIA case {case_no}: {case_title}")
    if len(case_links) == 0 and wanted_in_text != "":
        context.log.warning("Unlinked case number", name=name, value=wanted_in_text)

    context.emit(person)


def crawl(context: Context) -> None:
    page_url: str | None = context.data_url
    while page_url is not None:
        doc = context.fetch_html(page_url, cache_days=1)
        cards = h.xpath_elements(doc, "//div[@class='wanted-modal-card']")
        if len(cards) == 0:
            raise ValueError(f"No wanted persons found on {page_url}")
        for card in cards:
            crawl_person(context, card, page_url)
        next_links = h.xpath_strings(doc, "//a[@rel='next']/@href")
        page_url = urljoin(page_url, next_links[0]) if next_links else None

    assert_all_accepted(context, raise_on_unaccepted=False)
