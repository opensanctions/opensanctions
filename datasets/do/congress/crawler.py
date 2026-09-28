from itertools import count
from urllib.parse import urljoin

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


def discover_legislator_ids(context: Context) -> set[int]:
    ids: set[int] = set()
    # Search is required to run at all (an empty keyword returns zero results), but
    # the keyword is substring-matched against several fields including `funcion`,
    # and every role the source uses (Diputado/a, Senador/a, Institución del Estado)
    # contains an "a", so this one search returns every record regardless of name.
    for page in count(1):
        data = context.fetch_json(
            urljoin(context.data_url, "legisladores"),
            params={"page": page, "keyword": "a"},
            cache_days=1,
        )
        for result in data.pop("results"):
            ids.add(result.pop("legisladorId"))
        total = data.pop("total")
        page_size = data.pop("pageSize")
        context.audit_data(data, ignore=["page"])
        if page * page_size >= total:
            assert len(ids) == total, (len(ids), total)
            break
    return ids


def crawl_legislator(
    context: Context,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    legislador_id: int,
) -> None:
    data = context.fetch_json(
        urljoin(context.data_url, f"legislador/{legislador_id}"), cache_days=1
    )

    person = context.make("Person")
    person.id = context.make_slug("person", str(legislador_id))
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

    role = context.lookup("role", representation.pop("funcion"), warn_unmatched=True)
    if role is None or role.value is None:
        return
    position, categorisation = positions[role.value]
    if not categorisation.is_pep:
        return

    district = representation.pop("circunscripcion")
    district = context.lookup_value("district", district, district)
    constituency = ", ".join(
        part for part in (representation.pop("provincia"), district) if part is not None
    )

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        # The member's own dates: substitutes start mid-term, and early departures
        # (e.g. to an executive post) end before the term does.
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

    for legislador_id in sorted(discover_legislator_ids(context)):
        crawl_legislator(context, positions, legislador_id)
