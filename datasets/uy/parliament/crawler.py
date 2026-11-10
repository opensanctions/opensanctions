from lxml import html
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    name: str,
    party: str,
    department: str = "",
    url: str | None = None,
    office: str = "",
) -> None:
    # The sitting-members list gives no legislator ID, so both lists are keyed on
    # the name, which they spell identically.
    last_name, first_name = name.split(", ")
    person = context.make("Person")
    person.id = context.make_id(name)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("political", party.strip())
    # The presiding office, e.g. "Presidenta" for the Vice President in the Senate.
    person.add("position", office)
    person.add("sourceUrl", url)
    # Representatives and senators must be citizens: Constitution of Uruguay, Art. 90
    # and Art. 98. https://www.impo.com.uy/bases/constitucion/1967-1967
    person.add("citizenship", "uy")

    # Both lists hold current members only and give no dates.
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", department.strip())
    context.emit(occupancy)
    context.emit(person)


def crawl_chamber(context: Context, position: Entity, chamber: str) -> None:
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    url = f"{context.data_url}{chamber}/plenario/integracion"

    # The members elected to each seat ("Titulares"), including those on leave, e.g.
    # to serve as a minister.
    elected_url = f"{url}/titulares/json?page&_format=json"
    for row in zyte_api.fetch_json(context, elected_url, geolocation="UY"):
        # e.g. <a href="http://parlamento.gub.uy/camarasycomisiones/legisladores/9917">Andrade, Oscar</a>
        link = html.fragment_fromstring(row.pop("Nombres"))
        crawl_member(
            context,
            position,
            categorisation,
            name=h.element_text(link),
            party=row.pop("Lema"),
            department=row.pop("Departamento"),
            url=h.xpath_string(link, "./@href"),
        )
        context.audit_data(row)

    # The members sitting now ("Actuantes"), which adds the substitutes holding the
    # seats of members on leave. Members on both lists are emitted again with the same
    # IDs, which the export merges.
    sitting_url = f"{url}/json?page&_format=json"
    for row in zyte_api.fetch_json(context, sitting_url, geolocation="UY"):
        crawl_member(
            context,
            position,
            categorisation,
            name=row.pop("Nombres"),
            party=row.pop("Lema"),
            office=row.pop("Cargo"),
        )
        # The chamber and the substitution note ("Sustituye al Representante ...")
        # are not captured.
        context.audit_data(row, ignore=["Cuerpo", "Sustituciones"])


def crawl(context: Context) -> None:
    crawl_chamber(
        context,
        h.make_position(
            context,
            name="Senator of Uruguay",
            wikidata_id="Q19930721",
            country="uy",
            topics=["gov.national", "gov.legislative"],
            lang="eng",
        ),
        "senadores",
    )
    crawl_chamber(
        context,
        h.make_position(
            context,
            name="Member of the Chamber of Representatives of Uruguay",
            wikidata_id="Q19930720",
            country="uy",
            topics=["gov.national", "gov.legislative"],
            lang="eng",
        ),
        "representantes",
    )
