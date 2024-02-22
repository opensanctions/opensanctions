from datetime import datetime
from normality import collapse_spaces, slugify, stringify
from openpyxl import load_workbook
from pantomime.types import CSV
from typing import Dict, Optional
from rigour.ids.wikidata import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


FORMATS = ["%d/%m/%Y"]


def make_person_id(context: Context, id: str) -> str:
    return context.make_slug("person", id)


def crawl_person(context: Context, row: Dict[str, str]) -> str:
    person = context.make("Person")
    zvezo_id = row.pop("id_gni_live").strip()
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

    context.emit(person, target=True)
    #context.audit_data(row, [
    #    "is_first_time_in_office",
    #    "time_in_office",
    #])
    return zvezo_id, person.id


def en_label(institution_en: str, department_en: str, position_en: str) -> str:
    if position_en.lower() == "minister":
        label = f"Minister of {institution_en}"
        label = label.replace("Ministry of ", "")
    else:
        label = position_en

        if department_en:
            label += f", {department_en}"

        if (
            institution_en
            and slugify(institution_en) not in slugify(label)
        ):
            label += f", {institution_en}"
    return label


def si_label(institution_si: str, department_si: str, position_si: str) -> Optional[str]:
    label = position_si
    if department_si:
        label += f", {department_si}"
    if institution_si:
        label += f", {institution_si}"
    return label


def crawl_cv_entry(context: Context, entity_ids: Dict[str, str], row: Dict[str, str]):
    person = context.make("Person")
    svezo_id = row.pop("id_gni_live").strip()    
    person.id = entity_ids[svezo_id]

    institution_en = row.pop("institution_en")
    department_en = row.pop("institution_department_en")
    position_en = row.pop("position_en")

    institution_si = row.pop("institution_si")
    department_si = row.pop("institution_department_si")
    position_si = row.pop("position_si")

    part_of_cv_en = row.pop("part_of_cv_en")

    if part_of_cv_en == "Education":
        if institution_en:
            person.add("education", institution_en, lang="eng")
        if institution_si:
            person.add("education", institution_si, lang="slv")

    elif part_of_cv_en in {
        "Party position",
        "Work experience",
        "Advisory and supervisory functions",
    }:
        label_si = si_label(institution_si, department_si, position_si)
        label_en = en_label(institution_en, department_en, position_en)
       
        res = context.lookup("roughly_pep", label_en)
        if not res:
            return

        position = h.make_position(context, label_en, country="si")
        position.add("name", label_si, lang="slv")
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            return
        start_day = row.pop("start_day")
        start_date = h.parse_date(start_day, FORMATS)[0] if start_day else None
        if not start_date:
            start_year = row.pop("start_year")
            if start_year:
                start_date = start_year
                start_month = row.pop("start_month")
                if start_month:
                    start_date += "-" + start_month
        end_day = row.pop("end_day")
        end_date = h.parse_date(end_day, FORMATS)[0] if end_day else None
        if not end_date:
            end_year = row.pop("end_year")
            if end_year:
                end_date = end_year
                end_month = row.pop("end_month")
                if end_month:
                    end_date += "-" + end_month

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            False,
            start_date=start_date or None,
            end_date=end_date or None,
            categorisation=categorisation,
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person, target=True)
    elif part_of_cv_en == "Leisure activities":  # leisure activities
        return
    else:
        context.log.warning(f"Unhandled part of CV: {part_of_cv_en}")
        return


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
    entity_ids = {}
    path = context.fetch_resource("zvezoskop.xlsx", context.data_url)
    
    for row in excel_records(path, "persons_live", 16):
        svezo_id, entity_id = crawl_person(context, row)
        entity_ids[svezo_id] = entity_id

    for row in excel_records(path, "cv_live", 21):
        crawl_cv_entry(context, entity_ids, row)
