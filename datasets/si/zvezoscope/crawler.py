from normality import collapse_spaces, slugify
from pantomime.types import CSV
from typing import Dict, Optional
import csv

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


FORMATS = ["%d/%m/%Y"]


def make_person_id(context: Context, id: str) -> str:
    return context.make_slug("person", id)


def crawl_person(context: Context, row: Dict[str, str]) -> str:
    person = context.make("Person")
    internal_id = row.pop("id").strip()
    if not internal_id:
        return None, None
    wikidata_id = row.pop("wikidata_id").strip()
    if wikidata_id:
        person.id = wikidata_id
        person.add("wikidataId", wikidata_id)
    else:
        wikidata_id = None
        person.id = make_person_id(context, internal_id)
    person.add("name", row.pop("name"))
    person.add("birthDate", h.parse_date(row.pop("birth_date"), FORMATS))
    person.add("gender", row.pop("gender"))

    context.emit(person, target=True)

    return internal_id, wikidata_id


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
            # handle party name thrown in institution field. TODO handle in lookups
            and "mayor-of" not in slugify(label)
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


def crawl_cv_entry(context: Context, wikidata_ids: Dict[str, Optional[str]], row: Dict[str, str]):
    person = context.make("Person")
    internal_id = row.pop("person_id").strip()
    if not internal_id:
        return
    
    wikidata_id = wikidata_ids[internal_id]
    person.id = wikidata_id or make_person_id(context, internal_id)

    institution_en = row.pop("institution_en")
    department_en = row.pop("institution_department_en")
    position_en = row.pop("position_en")

    institution_si = row.pop("institution_si")
    department_si = row.pop("institution_department_si")
    position_si = row.pop("position_si")

    part_of_cv = row.pop("part_of_cv").lower()

    if part_of_cv == "izobraževanje":
        if institution_en:
            person.add("education", institution_en, lang="eng")
        if institution_si:
            person.add("education", institution_si, lang="slv")

    elif part_of_cv in {
        "strankarska pozicija",  # party position
        "delovne izkušnje",  # work experience
        "svetovalne in nadzorne funkcije etc.",  # advisory and supervisory functions, etc.
    }:
        label_si = si_label(institution_si, department_si, position_si)
        if position_en:
            lang = "eng"
            label = en_label(institution_en, department_en, position_en)
        elif label_si:
            lang = "slv"
            label = si_label(institution_si, department_si, position_si)
            print(label)
            return
        else:
            context.log.warning(f"Missing position for {internal_id} - {institution_en}/{institution_si}")
            return
        res = context.lookup("position", label)
        if res and res.label:
            label = res.label

        position = h.make_position(context, label, country="si", lang=lang)
        if lang == "eng":
            position.add("name", label_si, lang="slv")
        categorisation = categorise(context, position, is_pep=None)
        if not categorisation.is_pep:
            return
        
        start_date = h.parse_date(row.pop("start_day"), FORMATS)[0]
        if not start_date:
            start_year = row.pop("start_year")
            if start_year:
                start_date = start_year
                start_month = row.pop("start_month")
                if start_month:
                    start_date += "-" + start_month
        end_date = h.parse_date(row.pop("end_day"), FORMATS)[0]
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
    elif part_of_cv == "lastništvo podjetja":  # company ownership
        # print("OWNERSHIP", institution_en, position_en)
        return
    elif part_of_cv == "prostočasne aktivnosti":  # leisure activities
        return
    else:
        context.log.warning(f"Unhandled part of CV: {part_of_cv}")
        return

    context.emit(person, target=True)


def crawl(context: Context):
    wikidata_ids = {}
    path = context.fetch_resource("persons.csv", context.data_url)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            internal_id, wikidata_id = crawl_person(context, row)
            wikidata_ids[internal_id] = wikidata_id

    path = context.fetch_resource("cv-entries.csv", context.data_url)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_cv_entry(context, wikidata_ids, row)
