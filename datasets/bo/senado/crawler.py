import unicodedata
from collections import defaultdict
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The Senate has 36 seats (4 per department × 9 departments). The /pleno endpoint
# returns 72 records — 36 titulares + 36 suplentes — distinguished by an es_titular
# catalog id rather than a boolean: 83 = titular, 84 = suplente. We emit only the
# titulares and assert that exactly 36 come back, so the crawler fails loudly if the
# catalog encoding changes or the chamber is reapportioned.
TITULAR = 83
EXPECTED_TITULARES = 36

# The /pleno endpoint carries name, party, department and board role but no birth
# data. The companion /senadores endpoint adds date and place of birth; its records
# do not share ids with /pleno, so we join them by normalised full name.
ENRICHMENT_URL = "https://apisi.senado.gob.bo/page/senadores"


def normalise_name(nombre: str, apellidos: str) -> str:
    """Accent- and case-insensitive key for joining the two endpoints by name."""
    text = unicodedata.normalize("NFKD", f"{nombre} {apellidos}")
    text = "".join(c for c in text if not unicodedata.combining(c))
    return " ".join(text.lower().split())


def fetch_data(context: Context, url: str) -> list[dict[str, Any]]:
    """Fetch one of the API endpoints, returning its ``data`` list.

    The API responds with HTTP 201 on success and wraps the payload in
    ``{code, state, message, data}``; ``state`` must be true.
    """
    response = context.fetch_json(url)
    if not isinstance(response, dict) or response.get("state") is not True:
        raise ValueError(f"Unexpected API response from {url}: {response!r}")
    data = response["data"]
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError(f"Expected a non-empty data list from {url}")
    return data


def build_birth_index(
    context: Context, records: list[dict[str, Any]]
) -> dict[str, dict[str, str | None]]:
    """Map normalised full name → birth data from the enrichment endpoint.

    The endpoint lists each senator more than once (titular and suplente rows), so a
    name can map to several records; they agree on birth data, so we keep the first
    and only warn if two records for the same name disagree on the date of birth.
    """
    by_name: dict[str, dict[str, str | None]] = {}
    seen_dob: dict[str, str | None] = defaultdict(lambda: None)
    for record in records:
        key = normalise_name(record["nombre"], record["apellidos"])
        dob = record.get("fecha_nacimiento")
        if key in by_name:
            if dob is not None and seen_dob[key] not in (None, dob):
                context.log.warning(
                    "Conflicting birth dates for senator", name=key
                )
            continue
        by_name[key] = {
            "fecha_nacimiento": dob,
            "lugar_nacimiento": record.get("lugar_nacimiento"),
            "nombre": record["nombre"],
            "apellidos": record["apellidos"],
        }
        seen_dob[key] = dob
    return by_name


def crawl_senator(
    context: Context,
    row: dict[str, Any],
    birth_index: dict[str, dict[str, str | None]],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    nombre = row.pop("nombre")
    apellidos = row.pop("apellidos")
    brigada = row.pop("brigada_catalogo")
    department = brigada["name"] if isinstance(brigada, dict) else None

    person = context.make("Person")
    # No stable cross-endpoint id is exposed, so derive one from name + department.
    person.id = context.make_id(nombre, apellidos, department)

    # The enrichment endpoint provides properly-cased names (the /pleno roster is all
    # upper case); prefer it when the name joins, otherwise fall back to the roster.
    enriched = birth_index.get(normalise_name(nombre, apellidos))
    if enriched is None:
        context.log.warning("No birth data for senator", name=f"{nombre} {apellidos}")
        h.apply_name(person, first_name=nombre, last_name=apellidos)
    else:
        h.apply_name(
            person,
            first_name=enriched["nombre"],
            last_name=enriched["apellidos"],
        )
        h.apply_date(person, "birthDate", enriched["fecha_nacimiento"])
        person.add("birthPlace", enriched["lugar_nacimiento"])

    # Senators must be Bolivian citizens: citizenship is reserved to Bolivians and
    # includes the right to hold public office (Constitution arts. 144, 149-150).
    # https://pdba.georgetown.edu/Constitutions/Bolivia/bolivia09.html
    person.add("citizenship", "bo")
    bancada = row.pop("bancada")
    if isinstance(bancada, dict):
        person.add("political", bancada.get("nombre"))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # Senators are elected by department (4 per department).
    occupancy.add("constituency", department)

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(
        row,
        ignore=[
            "id",  # internal, not stable across endpoints
            "bancada_id",
            "brigada",  # numeric department code; name taken from brigada_catalogo
            "foto_perfil",
            "titular_id",
            "suplente_id",
            "cargo_directiva_id",
            "cargo_directiva_catalogo",  # board role (Presidente, etc.)
        ],
    )


def crawl(context: Context) -> None:
    members = fetch_data(context, context.data_url)
    titulares = [m for m in members if m.pop("es_titular") == TITULAR]
    if len(titulares) != EXPECTED_TITULARES:
        raise ValueError(
            f"Expected {EXPECTED_TITULARES} titular senators, got {len(titulares)}"
        )

    birth_index = build_birth_index(context, fetch_data(context, ENRICHMENT_URL))

    position = h.make_position(
        context,
        name="Member of the Chamber of Senators of Bolivia",
        country="bo",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q20081427",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in titulares:
        crawl_senator(context, row, birth_index, position, categorisation)
