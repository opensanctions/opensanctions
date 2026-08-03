import csv
import re

from normality import slugify
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
    # Jméno fyzické osoby
    # -> First name of the natural person

    first_name_field = row.pop("jmeno_fyzicke_osoby").strip('"').strip()
    # Cleaning here rather than via type.string lookup because we assemble full
    # names from these.
    first_names = h.multi_split(first_name_field, ["/"])

    # Příjmení fyzické osoby / Název právnické osoby / Označení nebo název entity
    # -> Last name of the natural person / Name of the legal entity / Designation or name of the entity
    name_field = row.pop(
        "prijmeni_fyzicke_osoby_nazev_pravnicke_osoby_oznaceni_nebo_nazev_entity"
    ).strip()

    # Datum narození fyzické osoby
    # -> Date of birth of the natural person
    birth_date = row.pop("datum_narozeni_fyzicke_osoby")

    # Státní příslušnost fyzické osoby / sídlo právnické osoby
    # -> Nationality of the natural person / registered office of the legal entity
    countries = row.pop("statni_prislusnost_fyzicke_osoby_sidlo_pravnicke_osoby")

    # Ustanovení předpisu Evropské unie, jehož skutkovou podstatu subjekt jednáním naplnil
    # -> Provision of the European Union regulation, the factual basis of which the subject fulfilled by action
    provision = row.pop(
        "ustanoveni_predpisu_evropske_unie_jehoz_skutkovou_podstatu_subjekt_jednanim_naplnil"
    )

    status = row.pop("stav_zapisu")
    if status not in (STATUS_VALID, STATUS_AMENDED, STATUS_CANCELLED):
        context.log.warning("Unknown entry status", status=status, name=name_field)

    # Poznámka ke stavu zápisu -> Note on the status of the entry
    status_note = row.pop("poznamka_ke_stavu_zapisu").strip()
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
    # Datum zápisu či jeho změny -> Date of the entry or its amendment
    h.apply_date(sanction, "startDate", row.pop("datum_zapisu_ci_jeho_zmeny"))
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

    # Popis postižitelného jednání -> Description of punishable conduct
    sanction.add("reason", row.pop("popis_postizitelneho_jednani"), lang="ces")

    # Uplatňovaná omezující opatření -> Restrictive measures applied
    sanction.add("provisions", row.pop("uplatnovana_omezujici_opatreni"), lang="ces")

    # Právní předpis zápisu
    # -> Legal instrument of the entry (a government resolution)
    sanction.add("recordId", row.pop("pravni_predpis_zapisu"), lang="ces")

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

    with open(path, encoding="utf-8") as fh:
        for record in csv.DictReader(fh):
            row = dict()
            for header, value in record.items():
                key = slugify(header, "_")
                if key is None:
                    raise ValueError(f"Blank column heading: {header!r}")
                if value is None:
                    raise ValueError(f"Missing value for column {header!r}")
                row[key] = value
            crawl_item(context, row)
