import io
import re
from csv import DictReader
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from zipfile import BadZipFile, ZipFile

from click.core import F
from zavod.shed.trans import (
    apply_translit_full_name,
    make_position_translation_prompt,
)
from zavod.stateful.positions import OccupancyStatus, categorise

from zavod import Context, Entity
from zavod import helpers as h

TRANSLIT_OUTPUT = {
    "eng": ("Latin", "English"),
}
POSITION_PROMPT = prompt = make_position_translation_prompt("cnr")


def fetch_latest_filing(
    context: Context,
    dates: List[Dict[str, object]],
) -> Tuple[Optional[str], Optional[Path]]:
    if not dates:
        return None, None

    # Iterate over all entries in reverse order
    for entry in reversed(dates):
        report_id = entry.get("stariIzvjestaj")
        if report_id != -1:  # Check for the first valid report
            url = f"https://portal.antikorupcija.me:9343/acamPublic/izvestajDetailsCSV.json?izvestajId={report_id}"
            # Ensure ID is an int and not a path traversal attack
            filename = f"{int(report_id)}.zip"
            zip_path = context.fetch_resource(filename, url)
            try:
                with ZipFile(zip_path, "r"):
                    return entry.get("datum"), zip_path
            except BadZipFile as e:
                context.log.info(
                    "Failed to open ZIP file. Skipping",
                    path=zip_path,
                    exception_str=str(e),
                )

    # If no valid entry is found
    return None, None


def read_csv_rows(context: Context, zip_path: Path, file_pattern: str) -> List[Dict]:
    """Generic CSV fetcher to retrieve CSV rows based on the file pattern."""

    with ZipFile(zip_path, "r") as zip:
        filtered_names = [
            name for name in zip.namelist() if name.startswith(file_pattern)
        ]
        file_name = next((name for name in filtered_names), None)
        if not file_name:
            context.log.warning(
                "No matching file found in the ZIP archive.", file_pattern
            )
            return []

        with zip.open(file_name) as zfh:
            fh = io.TextIOWrapper(zfh, encoding="utf-8-sig")
            reader = DictReader(fh, delimiter=",", quotechar='"')
            return list(reader)


def build_person_from_family_row(context: Context, row) -> Entity:
    first_name = row.pop("IME_CLANA_PORODICE")
    last_name = row.pop("PREZIME_CLANA_PORODICE")
    # We're dealing with: '-/-'
    maiden_name = row.pop("RODJENO_PREZIME_CLANA_PORODICE").strip("-").strip("/")
    # If the maiden name is actually a birthdate (i.e. only digits and dots),
    # treat it as the date of birth and clear the maiden name field
    dob = maiden_name if re.fullmatch(r"[.\d]+", maiden_name) else None
    maiden_name = None if dob else maiden_name

    city = row.pop("MESTO")
    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, city)
    entity.add("citizenship", row.pop("DRZAVLJANSTVO").split(","))
    h.apply_name(
        entity, first_name=first_name, last_name=last_name, maiden_name=maiden_name
    )
    h.apply_date(entity, "birthDate", dob)
    address = h.make_address(context, city=city, place=row.pop("BORAVISTE"), lang="cnr")
    h.copy_address(entity, address)
    context.audit_data(
        row,
        [
            "SRODSTVO",  # the family relationship, we use it elsewhere
            "FUNKCIONER_IME",
            "FUNKCIONER_PREZIME",
        ],
    )
    return entity


def crawl_relatives(context, person_entity, relatives):
    for row in relatives:
        relationship = row.pop("SRODSTVO")
        relative = build_person_from_family_row(context, row)
        if len(relative.caption) < 4:
            return
        context.emit(relative)

        # Emit a relationship
        rel = context.make("Family")
        rel.id = context.make_id(person_entity.id, relationship, relative.id)
        rel.add("relationship", relationship)
        rel.add("person", person_entity.id)
        rel.add("relative", relative.id)

        context.emit(rel)


def emit_affiliated_position(
    context: Context,
    *,
    person: Entity,
    function: str,
    affiliation_data: Dict[str, str],
    filing_date: str,
) -> None:
    """Creates Position and Occupancy after categorization.

    A position's name includes the function and the organization name.

    All positions (and Occupancies, Persons) are assumed to be Montenegrin
    """

    organization = affiliation_data.pop("ORGANIZACIJA")
    start_date = affiliation_data.pop("DATUM_POCETKA_OBAVLJANJA", None)
    end_date = affiliation_data.pop("DATUM_PRESTANKA_OBAVLJNJA")
    context.audit_data(
        affiliation_data,
        ignore=[
            "ORGANIZACIJA_IMENOVANJA",
            "ORGANIZACIJA_SAGLASNOSTI",
            "FUNKCIONER_IME",
            "FUNKCIONER_PREZIME",
        ],
    )

    position_name = f"{function}, {organization}"
    person.add("position", position_name)

    position = h.make_position(
        context,
        position_name,
        country="ME",
    )
    apply_translit_full_name(
        context, position, "cnr", position_name, TRANSLIT_OUTPUT, POSITION_PROMPT
    )

    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
    )
    # If the person should not be considered a PEP, don't emit the occupancy or position.
    if occupancy is None:
        return
    occupancy.add("declarationDate", filing_date)

    person.add("topics", "role.pep")
    context.emit(position)
    context.emit(occupancy)


# Verified Jan 2025 that when SRODSTVO == Funkcioner,
# FUNKCIONER_IME roughly equals IME_CLANA_PORODICE (first name)
# and FUNKCIONER_PREZIME roughly equals PREZIME_CLANA_PORODICE (last name)
def split_official_and_relatives(
    rows,
) -> Tuple[Optional[Dict[str, str]], List[Dict[str, str]]]:
    """Split family rows into the relatives and the official."""
    # SRODSTVO contains Funkcioner for the official, else the family relationship.
    officials = [row for row in rows if row.get("SRODSTVO") == "Funkcioner"]
    relatives = [row for row in rows if row.get("SRODSTVO") != "Funkcioner"]
    assert len(officials) <= 1, "Multiple 'Funkcioner' found in the relatives CSV."
    return officials[0] if officials else None, relatives


def crawl_person(context: Context, person_data) -> bool:
    """Crawl a person from the data.

    Returns true if a ZIP was read."""
    full_name = person_data.pop("imeIPrezime")
    dates = person_data.pop("izvjestajImovine")
    function_labels: List[str] = person_data.pop("nazivFunkcije")
    filing_date, zip_path = fetch_latest_filing(context, dates)

    family_rows = None
    function_rows = None
    if zip_path:
        # Relatives of a person
        family_rows = read_csv_rows(context, zip_path, "csv_clanovi_porodice_")
        # Function that this person holds
        function_rows = read_csv_rows(context, zip_path, "csv_funkcije_funkcionera_")
    official_row, relatives_rows = split_official_and_relatives(family_rows)

    if official_row:
        person = build_person_from_family_row(context, official_row)
    else:
        person = context.make("Person")
        # TODO(Leon Handreke): I think we should be passing *function_labels here,
        # but that would mean a re-key.
        person.id = context.make_id(full_name, sorted(function_labels))
        h.apply_name(person, full_name)

    if function_rows:
        assert filing_date is not None
        for row in function_rows:
            function = row.pop("FUNKCIJA")
            emit_affiliated_position(
                context,
                person=person,
                function=function,
                affiliation_data=row,
                filing_date=filing_date,
            )
    else:
        for label in function_labels:
            position = h.make_position(context, label, country="ME")
            categorisation = categorise(context, position, is_pep=True)
            if not categorisation.is_pep:
                continue
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                no_end_implies_current=False,
                categorisation=categorisation,
                status=OccupancyStatus.UNKNOWN,
            )
            context.emit(position)
            if occupancy is not None:
                context.emit(occupancy)
            person.add("topics", categorisation.topics)

    if "gov.national" in set(person.get("topics")):
        crawl_relatives(context, person, relatives_rows)

    context.emit(person)

    # True if we got some valid rows from the Zip file
    return bool(relatives_rows or function_rows)


def crawl(context: Context):
    valid_zips = 0
    page = 0
    max_pages = 1200
    while True:
        context.log.info("Crawling index page", page=page)
        data_url = f"{context.data_url}?page={page}&size=20"
        doc = context.fetch_json(data_url.format(page=page))

        if not doc:  # Break if an empty list is returned
            context.log.info(f"Stopped at page {page}")
            break

        for person in doc:
            if crawl_person(context, person):
                valid_zips += 1

        page += 1
        if page >= max_pages:
            context.log.warning(
                f"Emergency exit: Reached the maximum page limit of {max_pages}."
            )
            break

        if not valid_zips:
            context.log.warning("No valid ZIP files found.")
