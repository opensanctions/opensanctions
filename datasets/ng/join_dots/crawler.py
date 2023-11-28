import openpyxl
from typing import Any, Dict, Generator, List, Optional, Set, Tuple
from collections import defaultdict
from normality import collapse_spaces, slugify
from datetime import datetime
from pantomime.types import XLSX
import re

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


# Member Of The Of The Senate Of Nigeria
# of the of the

REGEX_STATE_ASSEMBLY = re.compile(
    r"^Member Of The House Of Assembly ([\w/'-]+ ){0,2}[\w/'-]+$"
)


def clean_position(
    context: Context, position: str, district: str, name: str
) -> Optional[str]:
    if position is None:
        return None
    position = collapse_spaces(position)
    position = re.sub(r"Of The Of The", "of the", position)
    slug = slugify(position)

    # position: Member Of The House Of Assembly Orhionmwon, district: Kwara
    # means the person is in the Orhionmwon seat of the Kwara state house of assembly
    # e.g. member of the Jigawa State House of Assembly (Q59528329)

    if slug.startswith("member-of-the-house-of-assembly"):
        if not district:
            context.log.info(
                "No district for state assembly", position=position, name=name
            )
            return position

        if REGEX_STATE_ASSEMBLY.match(position):
            return f"Member of the {district} State House of Assembly"
        else:
            context.log.warning(
                "Cannot parse apparent state assembly", position=position
            )

    if slug.startswith("member-of-the-house-of-representatives"):
        # Q21290864
        return "Member of the House of Representatives of Nigeria"

    if slug.startswith("member-of-the-senate-of-nigeria"):
        # Q19822359
        return "Member of the Senate of Nigeria"

    res = context.lookup("position", position)
    if res is None:
        return position
    else:
        return res.name


def position_topics(position):
    if "President Federal Republic Of Nigeria" in position:
        return ["gov.national", "gov.head"]
    if position.startswith("Minister"):
        return ["gov.national", "gov.executive"]
    if position.startswith("Member of the House of Representatives"):
        return ["gov.national", "gov.legislative"]
    if position.startswith("Member of the Senate"):
        return ["gov.national", "gov.legislative"]
    if "State House of Assembly" in position:
        return ["gov.state", "gov.legislative"]
    return []


def parse_position_dates(string: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if string is None:
        return None, None

    start, end = string.split(" - ")
    start = None if start == "NA" else start
    end = None if end == "NA" else end
    return start, end


def crawl_pep(context: Context, row) -> Tuple[Optional[str], Optional[str]]:
    name = row.pop("name")
    birth_date = row.pop("date_of_birth", None)
    birth_date = birth_date.isoformat()[:10] if birth_date else None
    district = row.pop("district", None)

    entity = context.make("Person")
    entity.id = context.make_slug(name, birth_date, "district", district, strict=False)

    entity.add("name", name)
    entity.add("birthDate", birth_date)
    entity.add("gender", row.pop("gender", None))

    position_name = clean_position(context, row.pop("position"), district, name)
    if position_name:
        topics = position_topics(position_name)
        subnational_area = district if "gov.state" in topics else None
        position = h.make_position(
            context,
            position_name,
            country="NG",
            topics=topics,
            subnational_area=subnational_area,
        )
        categorisation = categorise(context, position, True)

        start_date, end_date = parse_position_dates(row.pop("period", None))
        if categorisation.is_pep:
            occupancy = h.make_occupancy(
                context,
                entity,
                position,
                no_end_implies_current=False,
                start_date=start_date,
                end_date=end_date,
                birth_date=birth_date,
                categorisation=categorisation,
            )

            if occupancy:
                context.emit(entity, target=True)
                context.emit(occupancy)
                context.emit(position)

                context.audit_data(row, ignore=["age"])
                return name, entity.id
    return None, None


def crawl_relative(context: Context, row, pep_ids):
    name = row.pop("name")
    pep_name = row.pop("pep_name")
    pep_id = pep_ids.get(slugify(pep_name), None)
    if pep_id is None:
        # In spot-checked cases, their positions were too old to be relevant
        context.log.info("PEP not found for relative", pep=pep_name, relative=name)
        return
    entity = context.make("Person")
    entity.id = context.make_slug(name, "of", pep_id)
    entity.add("name", name)
    entity.add("topics", "role.rca")

    relationship = row.pop("relationship", None)
    rel = context.make("Family")
    rel.id = context.make_slug(
        name, relationship or "relative", "of", pep_name, strict=False
    )
    rel.add("person", pep_id)
    rel.add("relative", entity)
    rel.add("relationship", relationship)

    context.emit(entity)
    context.emit(rel)


def worksheet_rows(sheet) -> Generator[Dict[str, Any], None, None]:
    headers: Optional[List[str]] = None
    for row in sheet.rows:
        cells = [c.value for c in row]
        if headers is None:
            headers = [slugify(h, sep="_") for h in cells]
            continue
        yield dict(zip(headers, cells))


def crawl(context: Context):
    pep_ids = {}
    dupes = set()

    peps_path = context.fetch_resource(
        "peps.xlsx", context.data_url + "datasets/nigeria/PEP_data.xlsx"
    )
    context.export_resource(peps_path, XLSX, title="PEPs source data")
    workbook = openpyxl.load_workbook(peps_path, read_only=True)
    for row in worksheet_rows(workbook.worksheets[0]):
        name, id = crawl_pep(context, row)
        if name is None:
            continue
        name_slug = slugify(name)
        if name_slug in pep_ids:
            context.log.info("Dropping name with duplicate entry", name=name_slug)
            dupes.add(name_slug)
            pep_ids.pop((name_slug))
        else:
            if name_slug not in dupes:
                pep_ids[name_slug] = id

    peps_path = context.fetch_resource(
        "pep_relatives.xlsx",
        context.data_url + "datasets/nigeria/PEP_relatives_data.xlsx",
    )
    context.export_resource(peps_path, XLSX, title="PEP Relatives source data")
    workbook = openpyxl.load_workbook(peps_path, read_only=True)
    for row in worksheet_rows(workbook.worksheets[0]):
        if row:
            crawl_relative(context, row, pep_ids)
