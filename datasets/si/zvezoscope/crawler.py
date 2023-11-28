from normality import collapse_spaces, slugify
from pantomime.types import CSV
from typing import Dict
import csv

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


FORMATS = ["%d/%m/%Y"]


def make_person_id(context: Context, id: str) -> str:
    return context.make_slug("person", id)


def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")
    person_id = row.pop("person_id").strip()

    if not person_id:
        return
    person.id = make_person_id(context, person_id)

    institution_en = row.pop("institution_en")
    department_en = row.pop("institution_department_en")
    position_en = row.pop("position_en")

    institution_si = row.pop("institution_si")
    department_si = row.pop("institution_department_si")
    position_si = row.pop("position_si")

    part_of_cv = row.pop("part_of_cv")

    if part_of_cv == "izobraževanje":
        if institution_en:
            person.add("education", institution_en, lang="eng")
        if institution_si:
            person.add("education", institution_si, lang="slv")

    if part_of_cv in {
        "strankarska pozicija",  # party position
        "delovne izkušnje",  # work experience
        "svetovalne in nadzorne funkcije etc.",  # advisory and supervisory functions, etc.
    }:
        if not position_en:
            return

        lang = "eng"

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
                and "mayor-of" not in slugify(label)
            ):
                label += f", {institution_en}"

        res = context.lookup("position", label)
        if res is None:
            # context.log.warning("Unknown position", label=label)
            print(label)
            return
        if not res.is_pep:
            return
        label = res.label or label

        position = h.make_position(context, label, country="si")
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
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)

    if "role.pep" not in person.get("topics"):
        person.add("topics", "poi")
    context.emit(person, target=True)


def crawl(context: Context):
    path = context.fetch_resource("cv-entries.csv", context.data_url)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
