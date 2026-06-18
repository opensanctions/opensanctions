from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# C_REAL (actual end of mandate) holds this placeholder while a senator is serving.
NO_DATE = "Sin Datos"

IGNORE_FIELDS = [
    "D_LEGAL",  # scheduled term start; we use D_REAL (actual swearing-in)
    "C_LEGAL",  # scheduled term end; we use C_REAL (actual end of mandate)
    "TELEFONO",  # private contact detail
    "FOTO",
    "FACEBOOK",
    "TWITTER",
    "INSTAGRAM",
    "YOUTUBE",
]


def crawl_senator(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(row.pop("ID"))
    h.apply_name(
        person, first_name=row.pop("NOMBRE"), last_name=row.pop("APELLIDO")
    )
    # Senators must have been citizens of the Nation for six years (Constitution of
    # Argentina, Art. 55). https://www.constituteproject.org/constitution/Argentina_1994
    person.add("citizenship", "ar")
    person.add("political", row.pop("PARTIDO O ALIANZA"))
    person.add("email", row.pop("EMAIL"))

    # C_REAL is the actual end of mandate; "Sin Datos" means the senator is serving.
    end_date = row.pop("C_REAL")
    if end_date == NO_DATE:
        end_date = None

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=row.pop("D_REAL"),
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("PROVINCIA"))
    # The parliamentary bloc (bloque) is distinct from party membership.
    occupancy.add("politicalGroup", row.pop("BLOQUE"))

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE_FIELDS)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url)
    rows = data["table"]["rows"]
    if not isinstance(rows, list) or len(rows) == 0:
        raise ValueError("Expected a non-empty list of senators")

    position = h.make_position(
        context,
        name="Member of the Argentine Chamber of Senators",
        country="ar",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18711738",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in rows:
        crawl_senator(context, row, position, categorisation)
