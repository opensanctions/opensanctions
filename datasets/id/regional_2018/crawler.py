import re
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional
import openpyxl
from pantomime.types import XLSX
from normality import slugify, stringify

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


REGEX_TITLE = re.compile(r"^(Drs\. |Dr\. |Ir\. |Hj\. |PROF\. |IRJEN POL\. \(Purn\) |Dra\. )*", re.IGNORECASE)


def worksheet_rows(sheet) -> Generator[Dict[str, Any], None, None]:
    headers: Optional[List[str]] = None
    for row in sheet.iter_rows(min_row=4):
        cells = [c.value for c in row]
        if headers is None:
            headers = [slugify(h, sep="_") for h in cells]
            continue
        yield dict(zip(headers, cells))


def clean_name(name: str) -> str:
    name = re.sub("^\d\. ", "", name)
    name = REGEX_TITLE.sub("", name).strip()
    name = name.split(",")[0]
    return name


def crawl_person(context: Context, name, jurisdiction, position_ind, position_eng, topics):
    entity = context.make("Person")
    entity.id = context.make_slug(jurisdiction, name)
    entity.add("name", name)

    position = h.make_position(
        context,
        f"{position_ind} {jurisdiction}",
        lang="ind",
        country="id",
        topics=topics,
    )
    position.add("description", position_eng)
    categorisation = categorise(context, position, True)
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        start_date="2018",
        no_end_implies_current=False,
        categorisation=categorisation,
    )

    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)


def crawl_pair(context: Context, row, head_ind, head_eng, deputy_ind, deputy_eng, jurisdiction_column, topics):
    head_name, deputy_name = row["nama_paslon"].split("/")
    head_name = clean_name(head_name)
    deputy_name = clean_name(deputy_name)
    jurisdiction = row[jurisdiction_column].title()
    crawl_person(context, head_name, jurisdiction, head_ind, head_eng, topics)
    crawl_person(context, deputy_name, jurisdiction, deputy_ind, deputy_eng, topics)
    

def crawl(context: Context):
    path = context.fetch_resource("individuals.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = openpyxl.load_workbook(path, read_only=True)
    for row in worksheet_rows(workbook["Pemilihan Gubernur"]):
        crawl_pair(
            context,
            row,
            "Gubernur",
            "Governor",
            "Wakil Gubernur",
            "Deputy Governor",
            "provinsi",
            ["gov.head", "gov.state"],
        )

    for row in worksheet_rows(workbook["Pemilihan Bupati atau Walikota"]):
        crawl_pair(
            context,
            row,
            "Bupati atau Walikota",
            "Regent or Mayor",
            "Wakil Bupati atau Wakil Walikota",
            "Deputy Regent or Deputy Mayor",
            "kabupaten_kota",
            ["gov.head", "gov.muni"],
        )
