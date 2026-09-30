from itertools import count
from urllib.parse import urljoin

from followthemoney.util import join_text
from normality import squash_spaces
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context
from zavod import helpers as h

# Fields of a legislator record that are deliberately not extracted.
IGNORE_LEGISLATOR = [
    "id",
    "legisladorId",
    "nombreCompleto",
    "telefonoOficina",
    "correoInstitucional",
]
IGNORE_REPRESENTATION = [
    "circunscripcionId",
    # "En Curso" even for members whose term ended years ago.
    "ejercicio",
    "funcionId",
    "nivelId",
    "nivelRepresentacion",
    "periodo",
    "provinciaId",
]


def crawl_legislator(
    context: Context,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    legislador_id: str,
) -> None:
    data = context.fetch_json(
        urljoin(context.data_url, f"legislador/{legislador_id}"), cache_days=1
    )

    person = context.make("Person")
    person.id = context.make_slug(legislador_id)
    h.apply_name(
        person,
        first_name=squash_spaces(data.pop("nombres")),
        last_name=squash_spaces(data.pop("apellidos")),
        lang="spa",
    )
    # Both deputies and senators must be Dominican citizens (Constitution of the
    # Dominican Republic, Art. 79 for senators, Art. 82 for deputies which
    # cross-references Art. 79):
    # https://drlawyer.com/espanol/leyes/constitucion-de-la-republica-dominicana/
    person.add("citizenship", "do")
    person.add("profession", data.pop("profesion"))

    party = data.pop("partido")
    if party is not None:
        person.add("political", party.pop("nombre"))

    representation = data.pop("representacion")

    role = context.lookup("role", representation.pop("funcion"))
    if role is None or role.value is None:
        return
    position, categorisation = positions[role.value]
    if not categorisation.is_pep:
        return

    district = representation.pop("circunscripcion")
    district_lookup = context.lookup("district", district)
    if district_lookup is not None:
        district = district_lookup.value
    constituency = join_text(representation.pop("provincia"), district, sep=", ")

    # The member's own dates: substitutes start mid-term, and early departures
    # (e.g. to an executive post) end before the term does.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=representation.pop("inicio"),
        end_date=representation.pop("fin"),
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    context.emit(occupancy)
    context.emit(person)

    context.audit_data(data, ignore=IGNORE_LEGISLATOR)
    context.audit_data(representation, ignore=IGNORE_REPRESENTATION)


def crawl(context: Context) -> None:
    deputy_position = h.make_position(
        context,
        "Member of the Chamber of Deputies of the Dominican Republic",
        country="do",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328590",
        lang="eng",
    )
    senator_position = h.make_position(
        context,
        "Member of the Senate of the Dominican Republic",
        country="do",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295132",
        lang="eng",
    )
    positions = {
        "deputy": (deputy_position, categorise(context, deputy_position)),
        "senator": (senator_position, categorise(context, senator_position)),
    }
    context.emit(deputy_position)
    context.emit(senator_position)

    # An empty keyword returns nothing, but the search also matches `funcion`, and
    # every role (Diputado/a, Senador/a, Institución del Estado) contains an "a".
    for page in count(1):
        data = context.fetch_json(
            urljoin(context.data_url, "legisladores"),
            params={"page": page, "keyword": "a"},
            cache_days=1,
        )
        for result in data["results"]:
            crawl_legislator(context, positions, str(result["legisladorId"]))
        if page * data["pageSize"] >= data["total"]:
            break
