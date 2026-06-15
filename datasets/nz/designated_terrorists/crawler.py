from itertools import chain
from typing import Iterator
from normality import slugify
from zavod import Context, helpers as h
from zavod.util import Element
import re

# It will match the following substrings: DD (any month) YYYY
DATE_PATTERN = r"\b(\d{1,2} (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{4})\b"
ALIAS_SPLITS = [
    "formerly designated under the description",
    "formerly designated under description",
    "also known as",
    "Also known as",
    "; and",
    ";",
]
PROGRAM_KEY = "NZ-UNSC1373"


def parse_table(
    table: Element,
) -> Iterator[dict[str, tuple[str | None, list[str | None] | None]]]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                headers.append(slugify(h.element_text(el)))
            continue

        cells = []
        for el in row.findall("./td"):
            for span in el.findall(".//span"):
                # add newline to split spans later if we want
                span.tail = "\n" + span.tail if span.tail else "\n"

            # there can be multiple links in the same cell
            a_tags = el.findall(".//a")
            if a_tags is None:
                cells.append((h.element_text(el), None))
            else:
                cells.append((h.element_text(el), [a.get("href") for a in a_tags]))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells) if hdr is not None}


def crawl_item(
    context: Context, input_dict: dict[str, tuple[str | None, list[str | None] | None]]
) -> None:
    # aliases will be either a list of size one or None if there is no aliases
    name, *aliases = h.multi_split(input_dict.pop("terrorist-entity")[0], ALIAS_SPLITS)
    alias_lists = [h.multi_split(alias, [", and", ","]) for alias in aliases]
    aliases = list(chain.from_iterable(alias_lists))

    organization = context.make("Organization")
    organization.id = context.make_slug(name)
    organization.add("topics", "sanction")

    organization.add("name", name)
    organization.add("alias", aliases)

    sanction = h.make_sanction(context, organization, program_key=PROGRAM_KEY)

    raw_initial_sanction_date, initial_statement_url = input_dict.pop(
        "date-of-designation-as-a-terrorist-entity-in-new-zealand-including-and-statement-of-case-for-designation"
    )

    assert raw_initial_sanction_date is not None
    initial_sanction_date = re.findall(DATE_PATTERN, raw_initial_sanction_date)[0]

    # There is only one date in this case
    h.apply_date(sanction, "startDate", initial_sanction_date)
    sanction.add("sourceUrl", initial_statement_url)

    raw_renew_sanction_dates, renew_statement_urls = input_dict.pop(
        "date-terrorist-designation-was-renewed-in-new-zealand-including-statement-of-case-for-renewal-of-designation"
    )

    assert raw_renew_sanction_dates is not None
    renew_sanction_dates = re.findall(DATE_PATTERN, raw_renew_sanction_dates)

    for renew_sanction_date in renew_sanction_dates:
        h.apply_date(sanction, "date", renew_sanction_date)

    assert renew_statement_urls is not None
    for renew_statement_url in renew_statement_urls:
        sanction.add("sourceUrl", renew_statement_url)

    context.emit(organization)
    context.emit(sanction)
    context.audit_data(input_dict)


def crawl(context: Context) -> None:
    response = context.fetch_html(context.data_url, absolute_links=True)

    table = h.xpath_element(response, ".//table")

    caption = table.findtext(".//caption/strong")
    assert (
        caption
        == "Alphabetical list of Designated Terrorist Entities in New Zealand pursuant to UNSC Resolution 1373"
    ), caption

    for item in parse_table(table):
        crawl_item(context, item)
