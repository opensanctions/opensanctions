import csv
import re
from pathlib import PurePath
from urllib.parse import urlparse

from banal import ensure_list
from rigour.mime.types import CSV
from rigour.names import pick_name

from zavod import Context, helpers as h

# detect anything more complex than word/word/word (and handle question mark woopsie)
REGEX_LAST_NAME = re.compile(r"^[\w\?]+( ?/\s*[\w\?]+)*$")


def crawl_item(context: Context, row: dict[str, str]) -> None:
    first_name_field = row.pop("first_name").strip('"').strip()
    # Cleaning here rather than via type.string lookup because we assemble full
    # names from these.
    first_names = h.multi_split(first_name_field, ["/"])

    name_field = row.pop("last_name_or_entity_name").strip()
    birth_date = row.pop("birth_date")
    countries = row.pop("nationality_or_registered_office")
    provision = row.pop("eu_provision")

    status = row.pop("entry_status")
    status_key = context.lookup_value("entry_status", status, warn_unmatched=True)

    # The note on a cancelled or amended entry states the date(s) on which that
    # took effect in Czech prose, e.g. "Zápis byl zrušen ke dni 24. února 2025
    # v souladu s § 7 ...". The dates are extracted by hand in the lookup so
    # that none of the dates in a note are missed or misinterpreted.
    status_note = row.pop("entry_status_note").strip()
    end_dates: list[str] = []
    amendment_dates: list[str] = []
    if len(status_note) > 0:
        note_res = context.lookup("status_note", status_note)
        if note_res is None:
            context.log.warning(
                "Extract the dates of this note into the status_note lookup",
                note=status_note,
                name=name_field,
            )
        else:
            end_dates = ensure_list(note_res.end_dates)
            amendment_dates = ensure_list(note_res.amendment_dates)
    if status_key == "cancelled" and len(end_dates) == 0:
        context.log.warning(
            "No cancellation date for a cancelled entry",
            note=status_note,
            name=name_field,
        )

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
    h.apply_date(sanction, "startDate", row.pop("entry_or_amendment_date"))
    sanction.add("status", status, lang="ces")
    sanction.add("summary", status_note, lang="ces")
    h.apply_dates(sanction, "endDate", end_dates)
    # An amended entry is superseded by a later, valid entry for the same
    # subject, so its dates state when it was amended, not when it ended.
    h.apply_dates(sanction, "modifiedAt", amendment_dates)

    if h.is_active(sanction):
        entity.add("topics", "sanction")

    sanction.add("reason", row.pop("offence_description"), lang="ces")
    sanction.add("provisions", row.pop("restrictive_measures"), lang="ces")
    # The legal act which made the entry, i.e. a government resolution.
    sanction.add("description", row.pop("entry_legal_regulation"), lang="ces")

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


def check_in_sync_with_pdf(context: Context, data_url: str) -> None:
    """Warn if the CSV on the open data page lags the PDF on the list page.

    Both files are published under a name which ends in the date of the version,
    e.g. Vnitrostatni_sankcni_seznam_2026_07_23. The open data version of the
    list has gone stale for a long time in the past, so make sure it keeps being
    updated along with the PDF, which is the primary publication of the list.
    """
    assert context.dataset.url is not None
    doc = context.fetch_html(context.dataset.url, absolute_links=True)
    pdf_url = h.xpath_string(
        doc,
        '//a[contains(@href, ".pdf")]'
        '/span[contains(text(), "Vnitrostátní sankční seznam")]/../@href',
    )
    pdf_name = PurePath(urlparse(pdf_url).path).stem
    csv_name = PurePath(urlparse(data_url).path).stem
    if pdf_name != csv_name:
        context.log.warning(
            "The CSV and the PDF version of the list are out of sync",
            pdf_url=pdf_url,
            csv_url=data_url,
        )


def crawl(context: Context) -> None:
    # First we find the link to the CSV file
    data_url = crawl_data_url(context)
    check_in_sync_with_pdf(context, data_url)
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
