from normality import collapse_spaces
from pantomime.types import CSV
from typing import Dict
import csv
import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.logic.pep import OccupancyStatus, categorise

FORMATS = ["%m/%d/%Y"]
# Match space before comma or no space after comma
REGEX_FIX_COMMA = re.compile(r"(\w)\s*,\s*(\w)")

# Abakaliki South (PDP) Member of the House of Assembly Ebonyi State
# Abia State Commissioner for Health

# (Adavi, Okehi)member of the house of Representatives of the Federal Republic of Nigeria
# Federal House of Rep, Ibarapa East/Ido Constituency, Oyo State
# Anambra East/West Constituency House of Representative, Anambra State
# Member of the House of Rep. of the FRN (Awka North, Awka South)
# Member of the House of Rep. of Bende
# Member Federal House of Representative (Badagry) Lagos State
# Member of the House of Reps. Federal Republic of Nigeria, Bakura, Maradun
# Member, Federal House of Representatives, ZANGO/BAURE constituency
# Member, House of Rep. (Federal), Apa/Agatu Constituency
# Member, House of Representative Abak/Etim Ekpo/Ika Federal Constituency
REGEX_HOUSE_REP = re.compile(
    r"(member(, federal|,| of( the))?|federal|constituency) house of rep(,|\.|s.|resentative)(?! (Deputy|Chairman))",
    re.IGNORECASE,
)

# not
# House of Representatives Deputy Chairman Army


def crawl_position(context: Context, entity: Entity, name: str):
    name = REGEX_FIX_COMMA.sub(r"\1, \2", name)
    name_lower = name.lower()

    if "candidate" in name_lower:
        entity.add("topics", "poi")
        entity.add("country", "ng")
        entity.add("position", name)
        return

    if REGEX_HOUSE_REP.search(name):
        entity.add("position", name)
        name = "Member of the Federal House of Representatives"

    if name.startswith("Former "):
        status = OccupancyStatus.ENDED
        name = name.replace("Former ", "")
    else:
        status = OccupancyStatus.UNKNOWN

    position = h.make_position(context, name, country="ng")
    categorisation = categorise(context, position, True)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            entity,
            position,
            False,
            status=status,
            categorisation=categorisation,
        )
        context.emit(position)
        context.emit(occupancy)
    else:
        entity.add("position", name)
        entity.add("topics", "poi")


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    identifier = row.pop("Unique Identifier")
    if not identifier:
        return
    entity.id = context.make_slug(identifier)
    h.apply_name(
        entity,
        first_name=row.pop("First Name"),
        middle_name=row.pop("Middle Name"),
        last_name=row.pop("Last Name"),
    )
    entity.add("title", collapse_spaces(row.pop("Title")))
    entity.add("gender", row.pop("Gender"))
    entity.add("birthDate", h.parse_date(row.pop("Date of Birth").strip(), FORMATS))
    entity.add("address", collapse_spaces(row.pop("Official Address")))
    # confirming if this assumption is ok:
    # entity.add("birthPlace", row.pop("State Of Origin"))

    previous_pos = collapse_spaces(row.pop("Previous Position")).strip()
    if previous_pos:
        crawl_position(context, entity, previous_pos)
    present_pos = collapse_spaces(row.pop("Present Position")).strip()
    if present_pos:
        crawl_position(context, entity, present_pos)

    if not present_pos and not previous_pos:
        entity.add("topics", "role.pep")
        entity.add("country", "ng")

    context.emit(entity, target=True)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
