from lxml import etree
from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

GENDERS = {"Masculino": "male", "Femenino": "female"}


def current_party(deputy: etree._Element) -> str | None:
    """Return the party of the most recent (by start date) membership."""
    best_start = ""
    party: str | None = None
    for mil in deputy.findall(".//Militancia"):
        start = mil.findtext("FechaInicio") or ""
        name = mil.findtext("Partido/Nombre")
        if name and start >= best_start:
            best_start = start
            party = name
    return party


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Deputies of Chile",
        country="cl",
        wikidata_id="Q18067639",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    path = context.fetch_resource("deputies.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    h.remove_namespace(doc)

    deputies = doc.findall(".//Diputado")
    if not deputies:
        raise ValueError("No deputies found in the Chamber XML")

    for deputy in deputies:
        dip_id = deputy.findtext("Id")
        first = " ".join(
            p for p in (deputy.findtext("Nombre"), deputy.findtext("Nombre2")) if p
        )
        last = " ".join(
            p
            for p in (
                deputy.findtext("ApellidoPaterno"),
                deputy.findtext("ApellidoMaterno"),
            )
            if p
        )
        assert first or last, f"Deputy {dip_id} without a name"

        person = context.make("Person")
        person.id = context.make_slug(dip_id)
        h.apply_name(person, first_name=first, last_name=last, lang="spa")
        person.add("gender", GENDERS.get(deputy.findtext("Sexo") or ""))
        birth = deputy.findtext("FechaNacimiento")
        h.apply_date(person, "birthDate", birth[:10] if birth else None)
        person.add("political", current_party(deputy), lang="spa")
        # Deputies must be citizens with the right to vote (Constitution of Chile,
        # Article 48). https://www.constituteproject.org/constitution/Chile_2021
        person.add("citizenship", "cl")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)
