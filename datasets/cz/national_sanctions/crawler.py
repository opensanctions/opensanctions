import csv
import re

from rigour.mime.types import CSV
from rigour.names import pick_name

from zavod import Context, helpers as h

# detect anything more complex than word/word/word (and handle question mark woopsie)
REGEX_LAST_NAME = re.compile(r"^[\w\?]+( ?/\s*[\w\?]+)*$")
# The note on a cancelled or amended entry states the effective date(s) in prose,
# e.g. "Zápis byl zrušen ke dni 24. února 2025 v souladu s § 7 ...", sometimes
# with a numeric month. The formats are handled by the dataset `dates` config.
REGEX_STATUS_DATE = re.compile(r"ke dni\s+(\d{1,2}\.\s*\w+\.?\s*\d{4})")
# Stav zápisu -> Status of the entry
STATUS_VALID = "platný"
STATUS_AMENDED = "změněn"
STATUS_CANCELLED = "zrušen"


def crawl_item(context: Context, row: dict[str, str]) -> None:
    first_name_field = row.pop("first_name").strip('"').strip()
    # Cleaning here rather than via type.string lookup because we assemble full
    # names from these.
    first_names = h.multi_split(first_name_field, ["/"])

    name_field = row.pop("last_name_or_entity_name").strip()
    birth_date = row.pop("birth_date")
    countries = row.pop("nationality_or_country")
    provision = row.pop("eu_provision")

    status = row.pop("entry_status")
    if status not in (STATUS_VALID, STATUS_AMENDED, STATUS_CANCELLED):
        context.log.warning("Unknown entry status", status=status, name=name_field)

    status_note = row.pop("entry_status_note").strip()
    # One note states the wrong year, so the dates pass through a lookup.
    status_dates = [
        context.lookup_value("status_note_dates", text, text)
        for text in REGEX_STATUS_DATE.findall(status_note)
    ]

    if REGEX_LAST_NAME.match(name_field):
        names = h.multi_split(name_field, ["/"])
    else:
        res = context.lookup("name_notes", name_field)
        if res:
            names = res.names
        else:
            names = h.multi_split(name_field, ["/"])
            context.log.warning("Name field needs manual cleaning", name=name_field)

    if len(first_name_field) == 0:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name_field, first_name_field, countries)
        # There can be multiple names which are separated by /
        entity.add("name", names)
        entity.add("country", countries.split(", "), lang="ces")
    else:
        entity = context.make("Person")
        entity.id = context.make_id(name_field, first_name_field, countries)
        # There can be multiple names which are separated by /
        entity.add("lastName", names)
        entity.add("firstName", first_names)
        h.apply_name(
            entity,
            first_name=pick_name(first_names),
            last_name=pick_name(names),
        )
        h.apply_date(entity, "birthDate", birth_date.strip())
        entity.add("nationality", countries.split(", "), lang="ces")

    sanction = h.make_sanction(
        context,
        entity,
        source_program_key=provision,
        program_key=h.lookup_sanction_program_key(context, provision),
    )
    sanction.add("program", provision, lang="ces")
    h.apply_date(sanction, "startDate", row.pop("entry_date"))
    sanction.add("status", status, lang="ces")
    sanction.add("summary", status_note, lang="ces")
    if status == STATUS_CANCELLED:
        if len(status_dates) == 0:
            context.log.warning(
                "No cancellation date in status note", note=status_note, name=name_field
            )
        h.apply_dates(sanction, "endDate", status_dates)
    elif status == STATUS_AMENDED:
        # An amended entry is superseded by a later, valid entry for the same
        # subject, so the dates state when it was amended, not when it ended.
        h.apply_dates(sanction, "date", status_dates)
    elif len(status_dates) > 0:
        context.log.warning(
            "Unexpected date in the note of a valid entry",
            note=status_note,
            name=name_field,
        )

    if h.is_active(sanction):
        entity.add("topics", "sanction")

    sanction.add("reason", row.pop("conduct_description"), lang="ces")
    sanction.add("provisions", row.pop("restrictive_measures"), lang="ces")
    # The legal instrument of the entry, i.e. a government resolution.
    sanction.add("recordId", row.pop("entry_legal_instrument"), lang="ces")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_data_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    # The open data page links the list as a CSV attachment.
    return h.xpath_string(
        doc,
        '//a[contains(@href, ".csv")]'
        '/span[contains(text(), "Vnitrostátní sankční seznam")]/../@href',
    )


def crawl(context: Context) -> None:
    # First we find the link to the CSV file
    data_url = crawl_data_url(context)
    path = context.fetch_resource("source.csv", data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    columns = context.get_lookup("columns")
    with open(path, encoding="utf-8") as fh:
        for record in csv.DictReader(fh):
            row = dict()
            for header, value in record.items():
                # An unmapped heading is passed through under its source name, so
                # the pops in crawl_item fail or audit_data warns about it. A row
                # with fewer values than headings has None values.
                column = columns.get_value(header, header)
                assert column is not None and value is not None, record
                row[column] = value
            crawl_item(context, row)
