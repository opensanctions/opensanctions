"""
Crawler for active French senators.
"""

import csv
from typing import Dict

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

UNUSED_FIELDS = [
    "Qualité",
    "Groupe politique",
    "Type d'app au grp politique",
    "Commission permanente",
    "Circonscription",
    "Fonction au Bureau du Sénat",
    "PCS INSEE",
    "Catégorie professionnelle",
    "Description de la profession",
]


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    # Skip not currently serving senators
    status = row.pop("État")
    if status != "ACTIF":
        return
    # Unique senator ID (note: *not* a national ID number)
    senid = row.pop("Matricule")
    family_name = row.pop("Nom usuel")
    given_name = row.pop("Prénom usuel")
    birth_date = row.pop("Date naissance")
    email = row.pop("Courrier électronique")
    context.log.debug(f"Active senator {senid}: {given_name} {family_name}")
    # Hopefully, death date is not relevant for sitting senators
    _ = row.pop("Date de décès")
    # Ignore various other fields
    context.audit_data(row, UNUSED_FIELDS)
    # Make a PEP entity
    position = h.make_position(
        context,
        name="Senator of the French Fifth Republic",
        country="fr",
        topics=["gov.national", "gov.legislative"],
    )
    # is_pep=True because we expect all senators to be PEPs
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        # Unlikely that this would ever happen!
        context.log.warning(f"Senator {given_name} {family_name} is not PEP")
        return
    person = context.make("Person")
    person.id = context.make_slug(senid)
    context.log.debug(f"Unique ID {person.id}")
    h.apply_name(person, given_name=given_name, last_name=family_name, lang="fra")
    if email and email != "Non public":
        person.add("email", email)
    if birth_date:
        # Luckily they are all consistently formatted!
        person.add("birthDate", birth_date)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    """Retrieve list of senators as CSV and emit PEP entities for
    currently serving senators."""
    path = context.fetch_resource("senators.csv", context.data_url)
    with open(path, "rt", encoding="cp1252") as infh:
        decomment = (spam for spam in infh if spam[0] != "%")
        for row in csv.DictReader(decomment):
            crawl_row(context, row)

    # NOTE: To get start dates we need this auxiliary CSV:
    # https://data.senat.fr/data/senateurs/ODSEN_ELUSEN.csv
