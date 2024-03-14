"""
Crawler for active French mayors.
"""

import csv
from typing import Dict

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

UNUSED_FIELDS = [
    "Code sexe",
    "Code de la catégorie socio-professionnelle",
    "Libellé de la catégorie socio-professionnelle",
    "Date de début de la fonction",
]
DATE_FORMATS = ["%d/%m/%Y"]


def crawl_row(
    context: Context,
    row: Dict[str, str],
):
    """Process one row of the CSV data"""
    # Get infos
    family_name = row.pop("Nom de l'élu").strip()
    given_name = row.pop("Prénom de l'élu").strip()
    munid = row.pop("Code de la commune").strip()
    municipality = row.pop("Libellé de la commune").strip()
    birth_date = row.pop("Date de naissance").strip()
    start_date = row.pop("Date de début du mandat").strip()
    depid = row.pop("Code du département").strip()
    cspid = row.pop("Code de la collectivité à statut particulier").strip()
    departement = row.pop("Libellé du département").strip()
    cspname = row.pop("Libellé de la collectivité à statut particulier").strip()
    # Technically Martinique, Guyana, etc, are not départements but
    # they are subnational areas so we'll treat them the same
    subnational_area_id = depid or cspid
    subnational_area = departement or cspname
    context.audit_data(row, UNUSED_FIELDS)

    # Make a PEP entity
    person = context.make("Person")
    person.id = context.make_id(
        subnational_area_id, munid, family_name, given_name, birth_date
    )
    context.log.debug(f"Unique ID {person.id}")
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="fra")
    if birth_date:
        person.add("birthDate", h.parse_date(birth_date, DATE_FORMATS))
    position = h.make_position(
        context,
        name=f"Mayor of {municipality}",
        country="fr",
        subnational_area=subnational_area,
        topics=["gov.muni"],
    )

    # is_pep=True because we expect all mayors to be PEPs
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        context.log.warning(f"Mayor {given_name} {family_name} is not PEP")
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        start_date=h.parse_date(start_date, DATE_FORMATS)[0] if start_date else None,
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.log.debug(f"Emitting PEP entities for {given_name} {family_name}")
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    """Retrieve list of mayors as CSV and emit PEP entities."""
    path = context.fetch_resource("elus-maires.csv", context.data_url)
    with open(path, "rt", encoding="utf-8") as infh:
        for row in csv.DictReader(infh):
            crawl_row(context, row)
