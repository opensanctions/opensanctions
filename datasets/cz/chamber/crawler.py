import csv

from datetime import datetime
from zipfile import ZipFile
from typing import Dict, List

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, get_after_office

POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_DATE = datetime.now() - get_after_office(POSITION_TOPICS)


def parse_unl_file(file_path, encoding="windows-1250"):
    """Parse UNL format file (pipe-delimited, backslash-escaped)"""
    with open(file_path, "r", encoding=encoding) as fh:
        reader = csv.reader(fh, delimiter="|", escapechar="\\")
        for row in reader:
            yield [None if field == "" else field for field in row]


def load_osoby(file_path) -> Dict[str, Dict]:
    """
    Load persons table

    Schema:
    0: id_osoba (person_id)
    1: pred (before - title before name)
    2: prijmeni (surname)
    3: jmeno (name)
    4: za (for - title after name)
    5: narozeni (birth date)
    6: pohlavi (sex - M/other)
    7: zmena (change date)
    8: umrti (death date)
    """
    osoby = {}
    for row in parse_unl_file(file_path):
        if not row or not row[0]:
            continue
        osoby[row[0]] = {
            "id_osoba": row[0],
            "prefix": row[1] if len(row) > 1 else None,
            "last_name": row[2] if len(row) > 2 else None,
            "first_name": row[3] if len(row) > 3 else None,
            "suffix": row[4] if len(row) > 4 else None,
            "birth_date": row[5] if len(row) > 5 else None,
            "gender": row[6] if len(row) > 6 else None,
            "death_date": row[8] if len(row) > 8 else None,
        }
    return osoby


def load_zarazeni(file_path) -> List[Dict]:
    """
    Load classification/membership table

    Schema:
    0: id_osoba (person_id)
    1: id_of (organ or function ID)
    2: cl_funkce (cl_function - 0=membership, 1=function)
    3: od_o (from_o - inclusion from)
    4: do_o (to_o - inclusion to, NULL=current)
    5: od_f (from_f - mandate from)
    6: do_f (to_f - mandate to)
    """
    zarazeni = []
    for row in parse_unl_file(file_path):
        if not row or not row[0]:
            continue
        zarazeni.append(
            {
                "id_osoba": row[0],
                "id_of": row[1] if len(row) > 1 else None,
                "cl_funkce": row[2] if len(row) > 2 else None,
                "od_o": row[3] if len(row) > 3 else None,
                "do_o": row[4] if len(row) > 4 else None,
                "od_f": row[5] if len(row) > 5 else None,
                "do_f": row[6] if len(row) > 6 else None,
            }
        )
    return zarazeni


def load_poslanec(file_path) -> Dict[str, List[Dict]]:
    """
    Load MP table

    Schema:
    0: id_poslanec (MP_id)
    1: id_osoba (person_id)
    2: id_kraj (region_id)
    3: id_kandidatka (candidate_id - electoral party)
    4: id_obdobi (period_id - electoral period)
    5: web (website)
    6: ulice (street)
    7: obec (village)
    8: psc (zip code)
    9: email (e-mail)
    10: telefon (phone)
    11: fax
    12: psp_telefon (psp_phone)
    13: facebook
    14: foto (photo)
    """
    poslanec: dict[str, list[dict[str, str | None]]] = {}
    for row in parse_unl_file(file_path):
        if not row or len(row) < 2 or not row[1]:
            continue

        id_osoba = row[1]
        if id_osoba not in poslanec:
            poslanec[id_osoba] = []

        poslanec[id_osoba].append(
            {
                "id_poslanec": row[0],
                "id_osoba": row[1],
                "id_obdobi": row[4] if len(row) > 4 else None,
                "web": row[5] if len(row) > 5 else None,
                "email": row[9] if len(row) > 9 else None,
                "telefon": row[10] if len(row) > 10 else None,
                "facebook": row[13] if len(row) > 13 else None,
            }
        )
    return poslanec


def crawl_person(
    context: Context,
    osoby: Dict,
    zarazeni: List[Dict],
    poslanec: Dict,
):
    # Get electoral period IDs directly from poslanec table
    electoral_period_ids = set()
    for person_records in poslanec.values():
        for record in person_records:
            if record.get("id_obdobi"):
                electoral_period_ids.add(record["id_obdobi"])

    for id_osoba, osoba in osoby.items():
        # Check if person is in poslanec table (has been an MP)
        if id_osoba not in poslanec:
            continue
        # Find all MEMBERSHIPS (cl_funkce == 0) for this person in electoral periods
        person_memberships = [
            z
            for z in zarazeni
            if z["id_osoba"] == id_osoba
            and z["cl_funkce"] == "0"
            and z["id_of"] in electoral_period_ids
        ]

        # Create Person entity
        entity = context.make("Person")
        entity.id = context.make_id("person", id_osoba)
        h.apply_name(
            entity,
            first_name=osoba.get("first_name"),
            last_name=osoba.get("last_name"),
            prefix=osoba.get("prefix"),
            suffix=osoba.get("suffix"),
        )
        h.apply_date(entity, "birthDate", osoba.get("birth_date"))
        if osoba.get("gender") == "M":
            entity.add("gender", "male")
        else:
            entity.add("gender", "female")
        entity.add("citizenship", "cz")
        entity.add("sourceUrl", f"https://www.psp.cz/sqw/detail.sqw?id={id_osoba}")
        # Add parliament positions with tenure dates
        for membership in person_memberships:
            position_name = "Member of Parliament"
            end_date = membership["do_o"]
            if (
                end_date is not None
                and datetime.fromisoformat(end_date.split()[0]) < CUTOFF_DATE
            ):
                continue

            position = h.make_position(
                context,
                position_name,
                topics=POSITION_TOPICS,
                country="cz",
                wikidata_id="Q19803234",
            )
            categorisation = categorise(context, position, is_pep=True)
            if not categorisation.is_pep:
                continue

            occupancy = h.make_occupancy(
                context,
                entity,
                position,
                start_date=membership["od_o"],
                end_date=end_date,
            )

            if occupancy:
                context.emit(entity)
                context.emit(position)
                context.emit(occupancy)

        # # Add contact details
        # for mp_detail in poslanec[id_osoba]:
        #     if mp_detail.get("web"):
        #         entity.add("website", mp_detail["web"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.zip", context.data_url)
    work_dir = path.parent / "files"
    work_dir.mkdir(exist_ok=True)

    # Extract all files
    with ZipFile(path) as zip_file:
        for file_name in zip_file.namelist():
            context.log.info(f"Extracting {file_name}")
            zip_file.extract(file_name, work_dir)

    # Load tables
    osoby = load_osoby(work_dir / "osoby.unl")
    zarazeni = load_zarazeni(work_dir / "zarazeni.unl")
    poslanec = load_poslanec(work_dir / "poslanec.unl")

    # Get electoral period IDs from poslanec table for Chamber of Deputies
    period_ids_from_poslanec = set()
    for person_records in poslanec.values():
        for record in person_records:
            if record.get("id_obdobi"):
                period_ids_from_poslanec.add(record["id_obdobi"])

    crawl_person(context, osoby, zarazeni, poslanec)
