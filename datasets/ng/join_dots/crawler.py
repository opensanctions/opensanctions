import openpyxl
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from normality import collapse_spaces, slugify
from datetime import datetime
from pantomime.types import XLSX
import re

from zavod import Context
from zavod import helpers as h


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
    position = re.sub(r"of the of the", "of the", position, re.IGNORECASE)
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

    if slug.startswith("member-of-the-house-of-representatives-national-assembly"):
        # Q21290864
        return "Member of the House of Representatives of Nigeria"

    if slug.startswith("member-of-the-senate-of-nigeria-national-assembly"):
        # Q19822359
        return "Member of the Senate of Nigeria"

    return position


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
    end = None if end == "NA" else end
    return start, end


def worksheet_rows(sheet) -> List[Dict[str, Any]]:
    headers: Optional[List[str]] = None
    for row in sheet.rows:
        cells = [c.value for c in row]
        if headers is None:
            headers = [slugify(h, sep="_") for h in cells]
            continue
        yield dict(zip(headers, cells))


def crawl(context: Context):
    peps_path = context.fetch_resource(
        "peps.xlsx", context.data_url + "datasets/nigeria/PEP_data.xlsx"
    )
    context.export_resource(peps_path, XLSX, title="PEPs source data")

    workbook = openpyxl.load_workbook(peps_path, read_only=True)
    for row in worksheet_rows(workbook.worksheets[0]):
        name = row.pop("name")
        birth_date = row.pop("date_of_birth", None)
        birth_date = birth_date.isoformat()[:10] if birth_date else None
        district = row.pop("district", None)

        entity = context.make("Person")
        entity.id = context.make_slug(
            name, birth_date, "district", district, strict=False
        )

        entity.add("name", name)
        entity.add("birthDate", birth_date)
        entity.add("gender", row.pop("gender", None))

        position_name = clean_position(context, row.pop("position"), district, name)

        if position_name:
            position = h.make_position(
                context,
                position_name,
                country="NG",
                topics=position_topics(position_name),
            )
            start_date, end_date = parse_position_dates(row.pop("period", None))
            occupancy = h.make_occupancy(
                context,
                entity,
                position,
                no_end_implies_current=False,
                start_date=start_date,
                end_date=end_date,
                birth_date=birth_date,
            )

            if occupancy:
                context.emit(entity, target=True)
                context.emit(occupancy)
                context.emit(position)

        context.audit_data(row, ignore=["age"])
