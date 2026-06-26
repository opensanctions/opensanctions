import re
from typing import Any
import openpyxl
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


REGEX_TITLE = re.compile(
    r"^(Drs\. |Dr\. |Ir\. |Hj\. |PROF\. |IRJEN POL\. \(Purn\) |Dra\. )*", re.IGNORECASE
)


def clean_name(name: str) -> str:
    name = re.sub(r"^\d\. ", "", name)
    name = REGEX_TITLE.sub("", name).strip()
    name = name.split(",")[0]
    return name


def crawl_person(
    context: Context,
    name: str,
    jurisdiction: str,
    position_ind: str,
    position_eng: str,
    topics: list[str],
) -> None:
    entity = context.make("Person")
    entity.id = context.make_slug(jurisdiction, name)
    entity.add("name", name)
    entity.add("citizenship", "id")

    position = h.make_position(
        context,
        f"{position_ind} {jurisdiction}",
        lang="ind",
        country="id",
        topics=topics,
        subnational_area=jurisdiction,
    )
    position.add("description", position_eng)
    categorisation = categorise(context, position, default_is_pep=True)
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        start_date="2018",
        no_end_implies_current=False,
        categorisation=categorisation,
    )

    context.emit(entity)
    context.emit(position)
    if occupancy is not None:
        context.emit(occupancy)


def crawl_pair(
    context: Context,
    row: dict[str, Any],
    head_ind: str,
    head_eng: str,
    deputy_ind: str,
    deputy_eng: str,
    jurisdiction_column: str,
    topics: list[str],
) -> None:
    head_name, deputy_name = row["nama_paslon"].split("/")
    head_name = clean_name(head_name)
    deputy_name = clean_name(deputy_name)
    jurisdiction = row[jurisdiction_column].title()
    crawl_person(context, head_name, jurisdiction, head_ind, head_eng, topics)
    crawl_person(context, deputy_name, jurisdiction, deputy_ind, deputy_eng, topics)


def crawl(context: Context) -> None:
    path = context.fetch_resource("individuals.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = openpyxl.load_workbook(path, read_only=True)
    for row in h.parse_xlsx_sheet(context, workbook["Pemilihan Gubernur"], skiprows=3):
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

    for row in h.parse_xlsx_sheet(
        context, workbook["Pemilihan Bupati atau Walikota"], skiprows=3
    ):
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
