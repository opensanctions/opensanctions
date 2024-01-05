import re
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional
import openpyxl
from pantomime.types import XLSX
from normality import slugify, stringify

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


REGEX_TITLE = re.compile(r"^(Drs\. |Dr\. |Ir\. |Hj\. |PROF\. |IRJEN POL\. \(Purn\) )*", re.IGNORECASE)


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


def crawl_governor(context: Context, row: Dict[str, Any]):
    print(row)
    gov_name, deputy_name = row["nama_paslon"].split("/")
    gov_name = clean_name(gov_name)
    deputy_name = clean_name(deputy_name)
    province = row["provinsi"].title()

    governor = context.make("Person")
    governor.id = context.make_slug(province, gov_name)
    governor.add("name", gov_name)

    gov_position = h.make_position(
        context,
        f"Gubernur {province}",
        lang="ind",
        country="id",
        topics=["gov.head", "gov.state"],
    )
    gov_position.add("description", "Governor")
    gov_categorisation = categorise(context, gov_position, True)
    gov_occupancy = h.make_occupancy(
        context,
        governor,
        gov_position,
        start_date="2018",
        no_end_implies_current=False,
        categorisation=gov_categorisation,
    )

    context.emit(governor, target=True)
    context.emit(gov_position)
    context.emit(gov_occupancy)

    deputy = context.make("Person")
    deputy.id = context.make_slug(province, deputy_name)
    deputy.add("name", deputy_name)

    deputy_position = h.make_position(
        context,
        f"Wakil Gubernur {province}",
        lang="ind",
        country="id",
        topics=["gov.head", "gov.state"],
    )
    deputy_position.add("description", "Deputy Governor")
    deputy_categorisation = categorise(context, deputy_position, True)
    deputy_occupancy = h.make_occupancy(
        context,
        deputy,
        deputy_position,
        start_date="2018",
        no_end_implies_current=False,
        categorisation=deputy_categorisation,
    )

    context.emit(deputy, target=True)
    context.emit(deputy_position)
    context.emit(deputy_occupancy)


def crawl_regent_or_mayor(context: Context, row: Dict[str, Any]):
    print(row)


def crawl(context: Context):
    path = context.fetch_resource("individuals.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = openpyxl.load_workbook(path, read_only=True)
    for row in worksheet_rows(workbook["Pemilihan Gubernur"]):
        crawl_governor(context, row)

    # for row in worksheet_rows(workbook["Pemilihan Bupati atau Walikota"]):
    #    crawl_regent_or_mayor(context, row)
