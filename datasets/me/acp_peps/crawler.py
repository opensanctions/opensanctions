import io
from csv import DictReader
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from zavod import Context, helpers as h
from zavod.logic.pep import categorise, OccupancyStatus
from zipfile import ZipFile, BadZipFile
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

    latest_entry = max(
        dates, key=lambda item: datetime.strptime(item["datum"], "%Y-%m-%d")
    )
    latest_date: Optional[str] = latest_entry.get("datum")
    latest_report: Optional[int] = latest_entry.get("stariIzvjestaj")

    # Handle special case where `stariIzvjestaj` is `-1`
    if latest_report == -1:
        latest_report = None

    return latest_date, latest_report


def fetch_and_process_csv(context: Context, latest_report_id: int):
    # URL for fetching the CSV
    url = f"https://portal.antikorupcija.me:9343/acamPublic/izvestajDetailsCSV.json?izvestajId={latest_report_id}"

    # Fetch the ZIP file containing the CSV
    zip_path = context.fetch_resource(f"{latest_report_id}.zip", url)
    try:
        with ZipFile(zip_path, "r") as zip:
            # Find the file name that matches the pattern
            file_name = next(
                (
                    name
                    for name in zip.namelist()
                    if name.startswith("csv_funkcije_funkcionera_")
                ),
                None,
            )
            # Check if the file was found
            if not file_name:
                context.log.warning("No matching file found in the ZIP archive.")
                return []

            with zip.open(file_name) as zfh:
                fh = io.TextIOWrapper(zfh, encoding="utf-8-sig")
                reader = DictReader(fh, delimiter=",", quotechar='"')
                return list(reader)
    except BadZipFile:
        context.log.error(f"Failed to open {zip_path} as a ZIP file. Skipping.")
        return []


def crawl_person(context: Context, person):
    name = person.pop("imeIPrezime")
    position = person.pop("nazivFunkcije")
    dates = person.pop("izvjestajImovine")
    latest_date, report_date = extract_latest_filing(dates)
    report_details = fetch_and_process_csv(context, report_date)
    if not report_details:
        return

    for row in report_details:
        # Extract details from the CSV row
        # person_name = row.pop("FUNKCIONER_IME")
        # person_surname = row.pop("FUNKCIONER_PREZIME")
        # function = row.pop("FUNKCIJA")
        # organization = row.pop("ORGANIZACIJA")
        start_date = row.pop("DATUM_POCETKA_OBAVLJANJA", None)
        end_date = row.pop("DATUM_PRESTANKA_OBAVLJNJA")

    entity = context.make("Person")
    entity.id = context.make_id(name, position)
    entity.add("name", name)
    entity.add("topics", "role.pep")
    for pos in position:
        position = h.make_position(
            context,
            name=pos,
            country="ME",
            # lang="cnr",
        )
        entity.add("position", pos)

        apply_translit_full_name(
            context, position, "slk", pos, TRANSLIT_OUTPUT, POSITION_PROMPT
        )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            return

        occupancy = h.make_occupancy(
            context,
            entity,
            position,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else None,
            # no_end_implies_current=False,
            # categorisation=categorisation,
            status=OccupancyStatus.UNKNOWN,
        )
        occupancy.add("date", latest_date)

        context.emit(position)
        context.emit(occupancy)
        context.emit(entity, target=True)


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
            context.log.error(
                f"Emergency exit: Reached the maximum page limit of {max_pages}."
            )
            break
