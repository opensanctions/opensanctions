from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The roster lists each seat's titular deputy alongside their alternate (suplente).
# Titulars hold one of the leadership/member roles below; suplentes carry "SU".
SUPLENTE_ROLE = "SU"
TITULAR_ROLES = {
    "P",  # Presidente
    "VP",  # Vicepresidente
    "S",  # Secretario
    "PS",  # Prosecretario
    "D",  # Diputado
}

IGNORE_FIELDS = [
    "citizenId",
    "imageUrl",
    "isActive",
    "createdOn",
]


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    # Only titular deputies are emitted; their alternates (suplentes) are skipped.
    role = row.pop("role")
    if role == SUPLENTE_ROLE:
        return
    if role not in TITULAR_ROLES:
        raise ValueError(f"Unknown deputy role: {role!r}")

    record_id = row.pop("userDocument")
    party = row.pop("party")

    person = context.make("Person")
    person.id = context.make_slug(str(record_id))
    # Each deputy has a public profile page keyed by their numeric site id.
    person.add(
        "sourceUrl",
        "https://congresonacional.hn/congresistas/{}".format(row.pop("userId")),
    )
    h.apply_name(
        person,
        full=row.pop("displayName"),
        first_name=row.pop("name"),
        last_name=row.pop("lastname"),
    )
    # Deputies must be Honduran by birth ("hondureño por nacimiento"); naturalised
    # citizens are not eligible (Constitution of Honduras, Art. 198).
    # https://pdba.georgetown.edu/Constitutions/Honduras/hond82.html
    person.add("citizenship", "hn")
    person.add("political", party.pop("name"))
    # Some records list several addresses in one field, comma-separated and
    # occasionally with stray spaces inside the address.
    for email in row.pop("email").split(","):
        person.add("email", "".join(email.split()))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("departamento"))

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE_FIELDS)


def crawl(context: Context) -> None:
    # The roster is served as JSON by the site's API; the page itself loads it
    # client-side, so there is no longer usable data embedded in the HTML.
    data = context.fetch_json(context.data_url, cache_days=1)
    deputies = data["data"]["diputados"]

    position = h.make_position(
        context,
        name="Member of the National Congress of Honduras",
        country="hn",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q19300340",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in deputies:
        crawl_deputy(context, row, position, categorisation)
