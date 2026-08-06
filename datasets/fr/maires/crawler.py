"""
Crawler for active French mayors.
"""

import csv

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

UNUSED_FIELDS = [
    "Code de la catégorie socio-professionnelle",
    "Libellé de la catégorie socio-professionnelle",
    "Code de la collectivité à statut particulier",
    "Code du département",
]


def crawl_row(
    context: Context,
    row: dict[str, str],
) -> None:
    """Process one row of the CSV data"""
    # Get infos
    family_name = row.pop("Nom de l'élu").strip()
    given_name = row.pop("Prénom de l'élu").strip()
    munid = row.pop("Code de la commune").strip()
    municipality = row.pop("Libellé de la commune").strip()
    birth_date = row.pop("Date de naissance").strip()
    office_term_start_date = row.pop("Date de début du mandat").strip()
    function_start = row.pop(
        "Date de début de la fonction"
    ).strip()  # individual's mandate start date
    departement = row.pop("Libellé du département").strip()
    cspname = row.pop("Libellé de la collectivité à statut particulier").strip()
    # Technically Martinique, Guyana, etc, are not départements but
    # they are subnational areas so we'll treat them the same
    region = departement or cspname

    # Make a PEP entity
    person = context.make("Person")
    person.id = context.make_id(munid, family_name, given_name, birth_date)
    context.log.debug(f"Unique ID {person.id}")
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="fra")
    if birth_date:
        h.apply_date(person, "birthDate", birth_date)
    person.add("gender", row.pop("Code sexe"))
    # citizenhip required: https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000006389917
    person.add("citizenship", "fr")
    position = h.make_position(
        context,
        name=f"Mayor of {municipality}",
        country="fr",
        subnational_area=f"{municipality}, {region}",
        topics=["gov.muni", "gov.head"],
    )

    # is_pep=True because we expect all mayors to be PEPs
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        context.log.warning(f"Mayor {given_name} {family_name} is not PEP")
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=function_start,  # individual's mandate start date
        period_start=office_term_start_date,  # office term's start date
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.log.debug(f"Emitting PEP entities for {given_name} {family_name}")
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)
    context.audit_data(row, UNUSED_FIELDS)


def crawl(context: Context) -> None:
    """Retrieve list of mayors as CSV and emit PEP entities."""
    path = context.fetch_resource("elus-maires.csv", context.data_url)
    with open(path, encoding="utf-8") as infh:
        for row in csv.DictReader(infh, delimiter=";"):
            crawl_row(context, row)
