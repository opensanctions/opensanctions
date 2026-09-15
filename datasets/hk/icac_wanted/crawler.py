from normality import squash_spaces
from zavod.util import Element

from zavod import Context
from zavod import helpers as h


def parse_detail_table(doc: Element) -> dict[str, Element]:
    """Read the Name / Alias / Charge(s) table, leaving the cells unparsed.

    Several charges are listed as <br/>-separated lines, which element_text()
    would run together into one string.
    """
    cells: dict[str, Element] = {}
    rows = h.xpath_elements(
        doc, './/div[contains(@class, "wpInfoDetail")]//tr', expect_exactly=3
    )
    for row in rows:
        label = h.element_text(h.xpath_element(row, "./th")).rstrip(" :")
        cells[label] = h.xpath_element(row, "./td")
    return cells


def parse_details(doc: Element, url: str) -> dict[str, str]:
    """Read the "Personal Particular" block into one entry per label.

    Fields are separated by <br/>, so each direct text node is one line. A
    value too long for its line continues on the next one, which has no label.
    """
    block = h.xpath_element(doc, './/div[contains(@class, "wanted-bio")]')
    values: dict[str, str] = {}
    label: str | None = None
    for text in h.xpath_strings(block, "./text()"):
        line = squash_spaces(text)
        if not line:
            continue
        raw_label, separator, raw_value = line.partition(":")
        if not separator:
            # Unlabelled document numbers, masked at source like the labelled ones.
            if "No." in line:
                continue
            assert label is not None, (line, url)
            values[label] = f"{values[label]} {line}".strip()
            continue
        label = squash_spaces(raw_label)
        assert label not in values, (label, url)
        values[label] = squash_spaces(raw_value)
    assert len(values) > 0, url
    return values


def crawl_person(context: Context, url: str, last_name: str) -> None:
    doc = context.fetch_html(url, cache_days=1)

    cells = parse_detail_table(doc)
    name = h.element_text(cells.pop("Name"))
    alias = h.element_text(cells.pop("Alias"))
    charges = cells.pop("Charge(s)")
    context.audit_data(cells)

    person = context.make("Person")
    person.id = context.make_id(url)
    h.apply_name(person, full=name, last_name=last_name)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("country", "hk")
    person.add("sourceUrl", url)
    for charge in h.xpath_strings(charges, ".//text()"):
        person.add("notes", squash_spaces(charge))

    for alias_name in h.multi_split(alias, ["/"]):
        alias_prop = "alias" if " " in alias_name else "weakAlias"
        person.add(alias_prop, alias_name)

    for label, value in parse_details(doc, url).items():
        result = context.lookup("details", label)
        if result is None:
            context.log.warning("Unknown details label", label=label, url=url)
            continue
        prop = result.value
        if prop is None:
            continue
        if prop == "birthDate":
            h.apply_date(person, prop, value)
        elif prop in ("nationality", "spokenLanguage"):
            person.add(prop, h.multi_split(value, ["/", " and "]))
        else:
            person.add(prop, value)

    brief = h.xpath_element(doc, './/div[contains(@class, "caseBrief")]')
    person.add("notes", h.element_text(brief))

    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    links = h.xpath_elements(doc, './/a[contains(@href, "index_id_")]')
    assert len(links) > 0, "No wanted person links on the index page"

    seen: set[str] = set()
    for link in links:
        url = link.get("href")
        assert url is not None, "Wanted person link has no href"
        # Recent additions appear both in "Newly Wanted Person(s)" and the main list.
        if url in seen:
            continue
        seen.add(url)
        # The family name has its own span, the only reliable way to split these names.
        span = h.xpath_element(link, './/span[contains(@class, "hf_family_name")]')
        crawl_person(context, url, h.element_text(span))
