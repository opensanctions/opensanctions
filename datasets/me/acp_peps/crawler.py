import io
from csv import DictReader
from typing import List, Dict, Optional, Tuple
from zipfile import ZipFile, BadZipFile

from zavod import Context, Entity, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.trans import (
    apply_translit_full_name,
    make_position_translation_prompt,
)

TRANSLIT_OUTPUT = {
    "eng": ("Latin", "English"),
}
POSITION_PROMPT = prompt = make_position_translation_prompt("cnr")


def extract_latest_filing(
    dates: List[Dict[str, object]]
) -> Tuple[Optional[str], Optional[int]]:
    if not dates:
        return None, None

    # Iterate over all entries in reverse order
    for entry in reversed(dates):
        report_id = entry.get("stariIzvjestaj")
        if report_id != -1:  # Check for the first valid report
            return entry.get("datum"), report_id

    # If no valid entry is found
    return None, None


def fetch_csv_rows(context: Context, latest_report_id: int):
    # URL for fetching the CSV
    url = f"https://portal.antikorupcija.me:9343/acamPublic/izvestajDetailsCSV.json?izvestajId={latest_report_id}"

    # Fetch the ZIP file containing the CSV
    zip_path = context.fetch_resource(f"{latest_report_id}.zip", url)
    try:
        with ZipFile(zip_path, "r") as zip:
            # Find the file name that matches the pattern
            filtered_names = [
                name
                for name in zip.namelist()
                if name.startswith("csv_funkcije_funkcionera_")
            ]
            # Get the first matching name or None
            file_name = next((name for name in filtered_names), None)
            # Check if the file was found
            if not file_name:
                context.log.warning("No matching file found in the ZIP archive.")
                return []

            with zip.open(file_name) as zfh:
                fh = io.TextIOWrapper(zfh, encoding="utf-8-sig")
                reader = DictReader(fh, delimiter=",", quotechar='"')
                return list(reader)
    except BadZipFile:
        context.log.warning(f"Failed to open {zip_path} as a ZIP file. Skipping.")
        return []


def make_affiliation_entities(
    context: Context, entity: Entity, function, row: dict, filing_date
) -> List[Entity]:
    """Creates Position and Occupancy provided that the Occupancy meets OpenSanctions criteria.
    * A position's name include the title and the name of the legal entity
    * All positions (and Occupancies, Persons) are assumed to be Montenegrin
    """

    organization = row.pop("ORGANIZACIJA")
    start_date = row.pop("DATUM_POCETKA_OBAVLJANJA", None)
    end_date = row.pop("DATUM_PRESTANKA_OBAVLJNJA")
    context.audit_data(
        row, ignore=["ORGANIZACIJA_IMENOVANJA", "ORGANIZACIJA_SAGLASNOSTI"]
    )

    position_name = f"{function}, {organization}"
    entity.add("position", position_name)

    position = h.make_position(context, position_name, country="ME")
    apply_translit_full_name(
        context, position, "cnr", position_name, TRANSLIT_OUTPUT, POSITION_PROMPT
    )

    categorisation = categorise(context, position, is_pep=True)
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
        propagate_country=True,
        start_date=start_date,
        end_date=end_date,
    )
    entities = []
    if occupancy:
        occupancy.add("date", filing_date)
        entities.extend([position, occupancy])
    return entities


def crawl_person(context: Context, person):
    full_name = person.pop("imeIPrezime")
    # position = person.pop("nazivFunkcije")
    dates = person.pop("izvjestajImovine")
    filing_date, report_id = extract_latest_filing(dates)
    report_details = fetch_csv_rows(context, report_id)
    if not report_details:
        return

    position_entities = []
    for row in report_details:
        first_name = row.pop("FUNKCIONER_IME")
        last_name = row.pop("FUNKCIONER_PREZIME")
        function = row.pop("FUNKCIJA")
        entity = context.make("Person")
        entity.id = context.make_id(full_name, function)
        h.apply_name(
            entity,
            full_name,
            first_name,
            last_name,
        )
        entity.add("topics", "role.pep")
        position_entities.extend(
            make_affiliation_entities(context, entity, function, row, filing_date)
        )
        if position_entities:
            for position in position_entities:
                context.emit(position)
            context.emit(entity)


def crawl(context: Context):
    page = 0
    max_pages = 1200
    while True:
        data_url = f"https://obsidian.antikorupcija.me/api/ask-interni-pretraga/ank-izvjestaj-imovine/pretraga-izvjestaj-imovine-javni?page={page}&size=20"
        doc = context.fetch_json(data_url.format(page=page), cache_days=1)

        if not doc:  # Break if an empty list is returned
            context.log.info(f"Stopped at page {page}")
            break

        for person in doc:
            crawl_person(context, person)
        page += 1

        if page >= max_pages:
            context.log.warning(
                f"Emergency exit: Reached the maximum page limit of {max_pages}."
            )
            break
