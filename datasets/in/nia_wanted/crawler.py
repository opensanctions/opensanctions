from urllib.parse import urljoin

from lxml.etree import _Element as Element

from zavod import Context
from zavod import helpers as h


def strip_accused_number(value: str) -> str:
    """Strip a trailing accused number: "Masud K A (A-5)", "Niu Niu WA-1"."""
    for separator in ("(", " "):
        head, _, tail = value.rpartition(separator)
        number = tail.removeprefix("W").rstrip(").")
        if number.startswith("A-") and number[2:].isdigit():
            return head.rstrip(" ,")
    return value


def crawl_person(context: Context, card: Element, page_url: str) -> None:
    row: dict[str, str] = {}
    for field in h.xpath_elements(card, ".//div[@class='modal-detail-sec']/div"):
        label = h.element_text(h.xpath_element(field, "./div[contains(@class, 'lab')]"))
        value = h.xpath_element(field, "./div[contains(@class, 'val')]")
        row[label.rstrip(" :")] = h.element_text(value)

    name = row.pop("Name")
    aliases = row.pop("Aliases")
    person = context.make("Person")
    person.id = context.make_id(name, aliases, row.pop("Wanted in"))
    person.add("name", strip_accused_number(name))
    for alias in h.multi_split(aliases, "@"):
        alias = strip_accused_number(alias)
        person.add("alias" if " " in alias else "weakAlias", alias)
    h.copy_address(person, h.make_address(context, full=row.pop("Address")))
    person.add("topics", "wanted")
    status = row.pop("Accused Status")
    if status != "":
        person.add(
            "status",
            context.lookup_value("accused_status", status, warn_unmatched=True),
            original_value=status,
        )

    # One link per case the person is wanted in.
    for link in h.xpath_elements(card, ".//a[starts-with(@href, '/rc-')]"):
        case_url = urljoin(page_url, link.get("href"))
        person.add("sourceUrl", case_url)

    context.emit(person)
    context.audit_data(
        row,
        # Undated approximate ages ("31 years"): no birth date derivable.
        ignore=["Age/DOB (Approx)"],
    )


def crawl(context: Context) -> None:
    page_url: str | None = context.data_url
    while page_url is not None:
        doc = context.fetch_html(page_url, cache_days=1)
        for card in h.xpath_elements(doc, "//div[@class='wanted-modal-card']"):
            crawl_person(context, card, page_url)
        next_links = h.xpath_strings(doc, "//a[@rel='next']/@href")
        page_url = urljoin(page_url, next_links[0]) if next_links else None
