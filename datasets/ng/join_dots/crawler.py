import openpyxl
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from normality import collapse_spaces, slugify
from datetime import datetime
from pantomime.types import XLSX
from zavod import Context
from zavod import helpers as h

# position: Member Of The House Of Assembly Orhionmwon, district: Kwara
# means the person is in the Orhionmwon seat of the Kwara state house of assembly
# member of the Jigawa State House of Assembly (Q59528329)

# Member Of The Of The Senate Of Nigeria
# of the of the


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

        position_name = collapse_spaces(row.pop("position")) or None

        if position_name:
            position = h.make_position(context, position_name, country="NG")
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
