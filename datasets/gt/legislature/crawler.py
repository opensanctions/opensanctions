import re
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Each record links to the deputy's official profile, e.g.
# "https://www.congreso.gob.gt/perfil_diputado/929" — the numeric id is stable and
# official, so it keys the entity.
PROFILE_RE = re.compile(r"/perfil_diputado/(\d+)")
# Real birth dates are "DD-MM-YYYY"; missing ones are published as the placeholder
# "30-11--0001", which we skip rather than feed to apply_date.
BIRTH_RE = re.compile(r"^\d{2}-\d{2}-(19|20)\d{2}$")

# Contact details (private) and image/profile links are not emitted. "Unnamed: 9" is an
# occasional stray column (a duplicate of the bloc) in the source export.
IGNORE_FIELDS = [
    "Distrito al que representa:",
    "E-mail:",
    "Teléfono de la oficina:",
    "Dirección de la oficina:",
    "Imagen URL",
    "Perfil URL",
    "ImagenGithub",
    "Unnamed: 9",
]


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = row.pop("Nombre")
    born = row.pop("Fecha de nacimiento:")
    bloque = row.pop("Bloque al que representa")
    profile_url = row.get("Perfil URL") or ""

    match = PROFILE_RE.search(profile_url)
    if match is None:
        raise ValueError("No profile id for deputy %r (%r)" % (name, profile_url))

    person = context.make("Person")
    person.id = context.make_slug(match.group(1))
    person.add("name", name)
    if born is not None and BIRTH_RE.match(born):
        h.apply_date(person, "birthDate", born)
    # Deputies must be Guatemalan by origin ("guatemalteco de origen"); naturalised
    # citizens are not eligible (Political Constitution of Guatemala, Art. 162).
    # https://www.constituteproject.org/constitution/Guatemala_1993
    person.add("citizenship", "gt")

    # The bloc is published as "Party Name - ACRONYM"; store the party name. Deputies
    # sitting as independents ("Independiente - IND") have no party affiliation.
    party_name = bloque.rpartition(" - ")[0] or bloque
    if party_name != "Independiente":
        person.add("political", party_name)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE_FIELDS)


def crawl(context: Context) -> None:
    deputies = context.fetch_json(context.data_url, cache_days=1)
    if not isinstance(deputies, list) or len(deputies) < 120:
        raise ValueError("Expected a list of at least 120 deputies, got %r" % type(deputies))

    position = h.make_position(
        context,
        name="Member of the Congress of the Republic of Guatemala",
        country="gt",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18277108",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    for row in deputies:
        crawl_deputy(context, row, position, categorisation)
