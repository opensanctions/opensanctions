from datetime import datetime
from normality import collapse_spaces, slugify, stringify
from openpyxl import load_workbook
from pantomime.types import CSV
from typing import Dict, Optional
from rigour.ids.wikidata import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import OccupancyStatus, categorise


FORMATS = ["%d/%m/%Y"]


def make_person_id(context: Context, id: str) -> str:
    return context.make_slug("person", id)


def crawl_person(context: Context, row: Dict[str, str]) -> str:
    person = context.make("Person")
    zvezo_id = row.pop("id_gni_live")
    if not zvezo_id:
        return
    wikidata_id = row.pop("wikidata_id")
    if wikidata_id:
        wikidata_id = wikidata_id.strip()
    if wikidata_id and is_qid(wikidata_id):
        person.id = wikidata_id
        person.add("wikidataId", wikidata_id)
    else:
        wikidata_id = None
        person.id = make_person_id(context, zvezo_id)
    person.add("name", row.pop("name"))
    person.add("birthDate", row.pop("year"))
    person.add("gender", row.pop("gender"))
    person.add("sourceUrl", row.pop("zvezoskop_link"))
    person.add("political", row.pop("party_si") or None, lang="slv")
    person.add("political", row.pop("party_en") or None, lang="eng")
    asset_tracker_link = row.pop("asset_tracker_link")
    if asset_tracker_link and asset_tracker_link != "N/A":
        person.add(
            "notes",
            f"Also see this public official's assets tracked at {asset_tracker_link}",
        )

    context.audit_data(
        row,
        [
            "id",
            "is_first_time_in_office",
            "time_in_office",
            "position_si",
            "position_en",
            "institution_si",
            "institution_en",
            "year",
        ],
    )
    return zvezo_id, person


def en_label(institution_en: str, department_en: str, position_en: str) -> str:
    if position_en.lower() == "minister" and institution_en.lower().startswith(
        "ministry of"
    ):
        label = f"Minister of {institution_en}"
        label = label.replace("Ministry of ", "")
        return label
    # acronymns, and often party in institution
    if position_en == "MP":
        return "Member of the National Assembly of Slovenia"
    if position_en == "MEP":
        return "Member of the European Parliament"
    # Party in institution
    if "councillor in" in position_en.lower():
        return position_en

    label = position_en

    if "director" in position_en.lower() and department_en:
        label += f", {department_en}"

    if institution_en and slugify(institution_en) not in slugify(label):
        label += f", {institution_en}"
    return label


def si_label(institution_si: str, department_si: str, position_si: str) -> str:
    # Party in institution
    if position_si.lower() == "poslanec":
        return "Poslanec"
    if position_si.lower() == "poslanka":
        return "Poslanka"
    if "občinski svetnik v" in position_si.lower():
        return position_si

    label = position_si
    if "direktor" in position_si.lower() and department_si:
        label += f", {department_si}"
    if institution_si:
        label += f", {institution_si}"
    return label


def crawl_cv_entry(context: Context, entities: Dict[str, Entity], row: Dict[str, str]):
    svezo_id = row.pop("id_gni_live")
    person = entities[svezo_id]

    institution_en = row.pop("institution_en")
    institution_si = row.pop("institution_si")

    part_of_cv_en = row.pop("part_of_cv_en")

    if part_of_cv_en == "Education":
        if institution_en:
            person.add("education", institution_en, lang="eng")
        if institution_si:
            person.add("education", institution_si, lang="slv")

    elif part_of_cv_en == "Leisure activities":  # leisure activities
        return False
    elif part_of_cv_en not in {
        "Party position",
        "Work experience",
        "Advisory and supervisory functions",
    }:
        context.log.warning(f"Unhandled part of CV: {part_of_cv_en}")
        return False

    department_en = row.pop("institution_department_en")
    position_en = row.pop("position_en")
    department_si = row.pop("institution_department_si")
    position_si = row.pop("position_si")

    label_si = si_label(institution_si, department_si, position_si)
    label_en = en_label(institution_en, department_en, position_en)

    if "candidate" in label_en.lower():
        return False
    res = context.lookup("roughly_pep", label_en)
    if not res:
        return False

    position = h.make_position(context, label_en, country="si")
    position.add("name", label_si, lang="slv")
    categorisation = categorise(context, position, is_pep=None)
    if not categorisation.is_pep:
        return False
    start_day = row.pop("start_day")
    start_date = start_day
    if not start_date:
        start_year = row.pop("start_year")
        if start_year:
            start_date = start_year
            start_month = row.pop("start_month")
            if start_month:
                start_date += "-" + start_month
    end_day = row.pop("end_day")
    end_date = end_day
    if not end_date:
        end_year = row.pop("end_year")
        if end_year:
            end_date = end_year
            end_month = row.pop("end_month")
            if end_month:
                end_date += "-" + end_month
    assume_current = False
    if end_date == "2100":
        assume_current = True
        end_date = None

    # Temporarily ignore position age to load as much as possible
    # into Wikidata.
    if assume_current:
        status = OccupancyStatus.CURRENT
    elif end_date:
        status = OccupancyStatus.ENDED
    else:
        status = OccupancyStatus.UNKNOWN

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        assume_current,
        start_date=start_date or None,
        end_date=end_date or None,
        categorisation=categorisation,
        status=status,
    )
    if occupancy:
        notes_pos_si = row.pop("notes_position_si", None)
        if notes_pos_si:
            occupancy.add("summary", "Položaj: " + notes_pos_si, lang="slv")
        notes_pos_en = row.pop("notes_position_en", None)
        if notes_pos_en:
            occupancy.add("summary", "Position: " + notes_pos_en, lang="eng")
        notes_inst_si = row.pop("notes_institution_si", None)
        if notes_inst_si:
            occupancy.add("summary", "Institucija: " + notes_inst_si, lang="slv")
        notes_inst_en = row.pop("notes_institution_en", None)
        if notes_inst_en:
            occupancy.add("summary", "Institution: " + notes_inst_en, lang="eng")

        context.emit(position)
        context.emit(occupancy)
        context.emit(person, target=True)
        context.audit_data(
            row,
            [
                "id",
                "person_name",
                "start_month",
                "start_year",
                "end_month",
                "end_year",
                "institution_department_si",
                "institution_department_en",
                "part_of_cv",
            ],
        )
        return True


def header_names(cells, expected_columns: int):
    headers = []
    for idx, cell in enumerate(cells):
        if cell is None:
            if idx < expected_columns:
                raise ValueError(f"Missing header at column {idx}")
        headers.append(slugify(cell, "_"))
    return headers


def excel_records(path, sheet_name: str, expected_columns: int):
    wb = load_workbook(filename=path, read_only=True)
    sheet = wb[sheet_name]
    headers = None
    for idx, row in enumerate(sheet.rows):
        cells = [c.value for c in row][:expected_columns]
        if not any(cells):
            continue
        if headers is None:
            headers = header_names(cells, expected_columns)
            continue
        record = {}
        for header, value in zip(headers, cells):
            if isinstance(value, datetime):
                value = value.date()
            value = stringify(value)
            record[header] = value
        yield record


def crawl(context: Context):
    entities = {}
    all_zvezo_ids = set()
    emitted = set()
    path = context.fetch_resource("zvezoskop.xlsx", context.data_url)

    for row in excel_records(path, "persons_live", 16):
        svezo_id, entity = crawl_person(context, row)
        entities[svezo_id] = entity
        all_zvezo_ids.add(svezo_id)

    for row in excel_records(path, "cv_live", 21):
        zvezo_id = row["id_gni_live"]
        if crawl_cv_entry(context, entities, row):
            emitted.add(zvezo_id)

    not_emitted = all_zvezo_ids - emitted
    if not_emitted:
        context.log.warning("Not emitted persons", not_emitted=not_emitted)
