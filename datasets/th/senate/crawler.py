import csv
from typing import Any

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The catalogue serves the CSV in Windows-874 / TIS-620, not UTF-8.
ENCODING = "cp874"

# Civilian honorifics prefixing the name; ranks and academic titles are kept.
HONORIFICS = ("นางสาว", "นาย", "นาง")


def clean_name(raw: str) -> str:
    name = " ".join(raw.split())
    for honorific in HONORIFICS:
        if name.startswith(honorific):
            return name[len(honorific) :].strip()
    return name


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    code = row.pop("MEMBER_CODE")
    name = clean_name(row.pop("MEMBER_NAME"))
    assert name, f"Empty name for senator {code}"

    person = context.make("Person")
    # The code identifies the seat, not the person: a resigned senator and their
    # replacement share a code, so key on code + name.
    person.id = context.make_id(code, name)
    person.add("name", name, lang="tha")
    # A candidate for the Senate must be of Thai nationality by birth (Constitution of
    # Thailand 2017, Section 108(1)).
    # https://www.constituteproject.org/constitution/Thailand_2017
    person.add("citizenship", "th")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=row.pop("START_DATE") or None,
        end_date=row.pop("END_DATE") or None,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    group = row.pop("MEMBER_TYPE", None)
    if group:
        occupancy.add("description", group, lang="tha")

    context.audit_data(
        row,
        ignore=[
            "POSITION",
            "RESIGN",
            "COUNCIL_YEAR",
            "COUNCIL_NO",
            "COUNCIL_MEMBER",
        ],
    )
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Thailand",
        country="th",
        wikidata_id="Q21295152",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    path = context.fetch_resource("senators.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, encoding=ENCODING) as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ValueError("Senate CSV contained no rows")
    for row in rows:
        crawl_member(context, position, categorisation, row)
