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

ACTIVE_CAPTION = (
    "Alphabetical list of Designated Terrorist Entities in New Zealand "
    "pursuant to UNSC Resolution 1373"
)
ACTIVE_INITIAL_KEY = (
    "date-of-designation-as-a-terrorist-entity-in-new-zealand-"
    "including-and-statement-of-case-for-designation"
)
ACTIVE_RENEW_KEY = (
    "date-terrorist-designation-was-renewed-in-new-zealand-"
    "including-statement-of-case-for-renewal-of-designation"
)
EXPIRED_DATES_KEY = (
    "date-of-designation-or-renewal-as-a-terrorist-entity-in-new-zealand-"
    "including-and-statement-of-case-for-designati-and-the-attached-statement-of-case"
)
EXPIRED_END_KEY = (
    "date-terrorist-designation-was-allowed-to-expire-"
    "including-statement-of-case-for-expiration"
)
REVOKED_SECTION = "Revoked designations"
EXPIRED_SECTION = "Expired designations"


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
    context: Context,
    input_dict: dict[str, tuple[str | None, list[str | None] | None]],
    expired: bool,
) -> None:
    # aliases will be either a list of size one or None if there is no aliases
    name, *aliases = h.multi_split(input_dict.pop("terrorist-entity")[0], ALIAS_SPLITS)
    alias_lists = [h.multi_split(alias, [", and", ","]) for alias in aliases]
    aliases = list(chain.from_iterable(alias_lists))

    organization = context.make("Organization")
    organization.id = context.make_slug(name)
    organization.add("name", name)
    organization.add("alias", aliases)

    sanction = h.make_sanction(context, organization, program_key=PROGRAM_KEY)

    if expired:
        raw_dates, designation_urls = input_dict.pop(EXPIRED_DATES_KEY)
        assert raw_dates is not None
        designation_dates = re.findall(DATE_PATTERN, raw_dates)
        assert designation_dates, raw_dates
        h.apply_date(sanction, "startDate", designation_dates[0])
        for d in designation_dates[1:]:
            h.apply_date(sanction, "date", d)
        for url in designation_urls or []:
            sanction.add("sourceUrl", url)

        raw_end, expire_urls = input_dict.pop(EXPIRED_END_KEY)
        assert raw_end is not None
        end_dates = re.findall(DATE_PATTERN, raw_end)
        assert len(end_dates) == 1, raw_end
        h.apply_date(sanction, "endDate", end_dates[0])
        for url in expire_urls or []:
            sanction.add("sourceUrl", url)
    else:
        raw_initial_sanction_date, initial_statement_url = input_dict.pop(
            ACTIVE_INITIAL_KEY
        )

        assert raw_initial_sanction_date is not None
        initial_sanction_date = re.findall(DATE_PATTERN, raw_initial_sanction_date)[0]

        # There is only one date in this case
        h.apply_date(sanction, "startDate", initial_sanction_date)
        sanction.add("sourceUrl", initial_statement_url)

        raw_renew_sanction_dates, renew_statement_urls = input_dict.pop(
            ACTIVE_RENEW_KEY
        )

        assert raw_renew_sanction_dates is not None
        renew_sanction_dates = re.findall(DATE_PATTERN, raw_renew_sanction_dates)

        for renew_sanction_date in renew_sanction_dates:
            h.apply_date(sanction, "date", renew_sanction_date)

        assert renew_statement_urls is not None
        for renew_statement_url in renew_statement_urls:
            sanction.add("sourceUrl", renew_statement_url)

    if h.is_active(sanction):
        organization.add("topics", "sanction")

    context.emit(organization)
    context.emit(sanction)
    context.audit_data(input_dict)


def crawl(context: Context) -> None:
    response = context.fetch_html(context.data_url, absolute_links=True)

    active_seen = False
    for table in h.xpath_elements(response, ".//table"):
        section_headings = h.xpath_elements(table, "preceding-sibling::h2[1]")
        section = h.element_text(section_headings[0]) if section_headings else None
        if section == REVOKED_SECTION:
            continue
        if section == EXPIRED_SECTION:
            for item in parse_table(table):
                crawl_item(context, item, expired=True)
            continue
        if section is not None:
            raise ValueError(f"Unexpected section heading before table: {section!r}")

        caption = table.findtext(".//caption/strong")
        assert caption == ACTIVE_CAPTION, caption
        for item in parse_table(table):
            crawl_item(context, item, expired=False)
        active_seen = True

    assert active_seen, "Did not find active designations table"
