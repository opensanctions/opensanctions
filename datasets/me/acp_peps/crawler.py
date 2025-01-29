import io
from csv import DictReader
from typing import List, Dict, Optional, Tuple, Set
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


def fetch_csv(context: Context, report_id: int, file_pattern: str) -> List[Dict]:
    """Generic CSV fetcher to retrieve CSV rows based on the file pattern."""
    url = f"https://portal.antikorupcija.me:9343/acamPublic/izvestajDetailsCSV.json?izvestajId={report_id}"
    zip_path = context.fetch_resource(f"{report_id}.zip", url)

    try:
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
    except BadZipFile:
        context.log.warning(f"Failed to open {zip_path} as a ZIP file. Skipping.")
        return []


def crawl_relative(context, person_entity, relatives):
    for row in relatives:
        name = row.pop("FUNKCIONER_IME")
        surname = row.pop("FUNKCIONER_PREZIME")
        relative_name = row.pop("IME_CLANA_PORODICE")
        relative_surname = row.pop("PREZIME_CLANA_PORODICE")
        # Skip if it's a self-reference
        if name == relative_name and surname == relative_surname:
            continue
        maiden_name = row.pop("RODJENO_PREZIME_CLANA_PORODICE")
        relationship = row.pop("SRODSTVO")
        nationality = row.pop("DRZAVLJANSTVO")
        city = row.pop("MESTO")
        address = row.pop("BORAVISTE")

        # Emit a relative
        relative = context.make("Person")
        relative.id = context.make_id(relative_name, relative_surname, maiden_name)
        relative.add("address", address)
        relative.add("nationality", nationality)
        h.apply_name(
            relative,
            first_name=relative_name,
            last_name=relative_surname,
            maiden_name=maiden_name,
        )
        address_ent = h.make_address(
            context,
            full=address,
            city=city,
            lang="cnr",
        )
        h.copy_address(relative, address_ent)
        context.emit(relative)

        # Emit a relationship
        rel = context.make("Family")
        rel.id = context.make_id(person_entity.id, relationship, relative.id)
        rel.add("relationship", relationship)
        rel.add("person", person_entity.id)
        rel.add("relative", relative.id)

        context.emit(rel)
        context.audit_data(row)


def make_affiliation_entities(
    context: Context, entity: Entity, function, row: dict, filing_date, report_id
) -> Tuple[List[Entity], Set[str]]:  # List[Entity]:
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

    position = h.make_position(
        context,
        position_name,
        # topics=["gov.national"],  # for testing
        country="ME",
    )
    apply_translit_full_name(
        context, position, "cnr", position_name, TRANSLIT_OUTPUT, POSITION_PROMPT
    )

    categorisation = categorise(context, position, is_pep=True)
    categorisation_topics = set(categorisation.topics)

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
        # Switch to declarationDate, once it's introduced in FtM
        occupancy.add("date", filing_date)
        entities.extend([position, occupancy])
    return entities, categorisation_topics


def crawl_person(context: Context, person):
    full_name = person.pop("imeIPrezime")
    dates = person.pop("izvjestajImovine")
    filing_date, report_id = extract_latest_filing(dates)

    relatives_csv = fetch_csv(context, report_id, "csv_clanovi_porodice_")
    pep = [row for row in relatives_csv if row.get("SRODSTVO") == "Funkcioner"]
    relatives = [row for row in relatives_csv if row.get("SRODSTVO") != "Funkcioner"]

    if pep:
        official = pep[0]
        first_name = official.pop("FUNKCIONER_IME")
        last_name = official.pop("FUNKCIONER_PREZIME")
        city = official.pop("MESTO")
        entity = context.make("Person")
        entity.id = context.make_id(first_name, city)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
    else:
        function_label = person.pop("nazivFunkcije")
        entity = context.make("Person")
        entity.id = context.make_id(full_name, function_label)
        h.apply_name(entity, full_name)

    entity.add("topics", "role.pep")

    report_details = fetch_csv(context, report_id, "csv_funkcije_funkcionera_")
    position_entities = []
    categorisation_topics = set()

    if report_details:
        for row in report_details:
            function = row.pop("FUNKCIJA")
            entities, topics = make_affiliation_entities(
                context, entity, function, row, filing_date, report_id
            )
            position_entities.extend(entities)
            categorisation_topics.update(topics)
    else:
        function_label = person.pop("nazivFunkcije")
        position = h.make_position(context, function_label, country="ME")
        categorisation = categorise(context, position, is_pep=True)
        categorisation_topics.update(categorisation.topics)
        position_entities.append(position)

    if "gov.national" in categorisation_topics:
        crawl_relative(context, entity, relatives)

    if position_entities:
        for position in position_entities:
            context.emit(position)
        context.emit(entity)


def crawl(context: Context):
    page = 0
    max_pages = 1200
    while True:
        data_url = f"https://obsidian.antikorupcija.me/api/ask-interni-pretraga/ank-izvjestaj-imovine/pretraga-izvjestaj-imovine-javni?page={page}&size=20"
        doc = context.fetch_json(data_url.format(page=page))

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
