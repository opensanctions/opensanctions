import re
from itertools import chain

from zavod import Context, helpers as h
from zavod.util import Element
from zavod.extract import zyte_api

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
    "date_of_designation_as_a_terrorist_entity_in_new_zealand_"
    "including_and_statement_of_case_for_designation"
)
ACTIVE_RENEW_KEY = (
    "date_terrorist_designation_was_renewed_in_new_zealand_"
    "including_statement_of_case_for_renewal_of_designation"
)
EXPIRED_DATES_KEY = (
    "date_of_designation_or_renewal_as_a_terrorist_entity_in_new_zealand_"
    "including_and_statement_of_case_for_designati_and_the_attached_statement_of_case"
)
EXPIRED_END_KEY = (
    "date_terrorist_designation_was_allowed_to_expire_"
    "including_statement_of_case_for_expiration"
)
REVOKED_SECTION = "Revoked designations"
EXPIRED_SECTION = "Expired designations"
EMPTY_COLUMN_KEYS = ["column_0", "column_2", "column_4"]


def _cell_dates(cell: Element) -> list[str]:
    for br in h.xpath_elements(cell, ".//br"):
        br.tail = br.tail + "\n" if br.tail else "\n"
    return re.findall(DATE_PATTERN, h.element_text(cell))


def crawl_item(context: Context, row: dict[str, Element], expired: bool) -> None:
    name_cell = row.pop("terrorist_entity")
    # aliases will be either a list of size one or None if there is no aliases
    raw_name = h.element_text(name_cell)
    name, *aliases = h.multi_split(raw_name, ALIAS_SPLITS)
    alias_lists = [h.multi_split(alias, [", and", ","]) for alias in aliases]
    aliases = list(chain.from_iterable(alias_lists))

    organization = context.make("Organization")
    old_key = context.make_slug(name)
    new_key = context.make_id(raw_name)
    context.rekey(old_key, new_key)  # TODO: Remove in name migration step 3
    organization.id = new_key
    organization.add("name", name)
    organization.add("alias", aliases)
    original = h.Names(name=raw_name)
    suggested = h.Names()
    suggested.add("name", name)
    for alias in aliases:
        suggested.add("alias", alias)
    is_irregular, suggested = h.check_names_regularity(organization, suggested)
    h.review_names(
        context,
        organization,
        original=original,
        suggested=suggested,
        is_irregular=is_irregular,
        default_accepted=True,
    )

    sanction = h.make_sanction(context, organization, program_key=PROGRAM_KEY)

    if expired:
        designation_cell = row.pop(EXPIRED_DATES_KEY)
        designation_dates = _cell_dates(designation_cell)
        assert designation_dates, h.element_text(designation_cell)
        h.apply_date(sanction, "startDate", designation_dates[0])
        for d in designation_dates[1:]:
            h.apply_date(sanction, "date", d)
        for url in h.xpath_strings(designation_cell, ".//a/@href"):
            sanction.add("sourceUrl", url)

        end_cell = row.pop(EXPIRED_END_KEY)
        end_dates = _cell_dates(end_cell)
        assert end_dates, h.element_text(end_cell)
        # The expiry date is listed first. For some entities the source also
        # lists the renewal history in this cell (rather than in the
        # designation column); treat those trailing dates as renewals.
        h.apply_date(sanction, "endDate", end_dates[0])
        for d in end_dates[1:]:
            h.apply_date(sanction, "date", d)
        for url in h.xpath_strings(end_cell, ".//a/@href"):
            sanction.add("sourceUrl", url)
    else:
        initial_cell = row.pop(ACTIVE_INITIAL_KEY)
        initial_dates = _cell_dates(initial_cell)
        # There is only one date in this case
        h.apply_date(sanction, "startDate", initial_dates[0])
        for url in h.xpath_strings(initial_cell, ".//a/@href"):
            sanction.add("sourceUrl", url)

        renew_cell = row.pop(ACTIVE_RENEW_KEY)
        for d in _cell_dates(renew_cell):
            h.apply_date(sanction, "date", d)
        for url in h.xpath_strings(renew_cell, ".//a/@href"):
            sanction.add("sourceUrl", url)

    if h.is_active(sanction):
        organization.add("topics", "sanction")

    context.emit(organization)
    context.emit(sanction)
    context.audit_data(row, ignore=EMPTY_COLUMN_KEYS)


def crawl(context: Context) -> None:
    table_xpath = ".//table"
    response = zyte_api.fetch_html(
        context, context.data_url, unblock_validator=table_xpath, absolute_links=True
    )

    active_seen = False
    for table in h.xpath_elements(response, table_xpath):
        section_headings = h.xpath_elements(table, "preceding-sibling::h2[1]")
        section = h.element_text(section_headings[0]) if section_headings else None
        if section == REVOKED_SECTION:
            continue
        if section == EXPIRED_SECTION:
            for item in h.parse_html_table(table, index_empty_headers=True):
                crawl_item(context, item, expired=True)
            continue
        if section is not None:
            raise ValueError(f"Unexpected section heading before table: {section!r}")

        caption = table.findtext(".//caption/strong")
        assert caption == ACTIVE_CAPTION, caption
        for item in h.parse_html_table(table, index_empty_headers=True):
            crawl_item(context, item, expired=False)
        active_seen = True

    assert active_seen, "Did not find active designations table"
