"""
Crawler for active French senators.
"""

import csv
from typing import Dict, Iterator, List, Tuple, Optional
from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise

UNUSED_FIELDS = [
    "Groupe politique",
    "Type d'app au grp politique",
    "Commission permanente",
    "Circonscription",
    "Fonction au Bureau du Sénat",
    "PCS INSEE",
    "Catégorie professionnelle",
    "Description de la profession",
]
UNUSED_MANDATE_FIELDS = [
    "Qualité",
    "Nom usuel",
    "Prénom usuel",
    "État Sénateur",
    "Identifiant mandat",
    "Date d'élection",
    "Année de début de mandat","Motif début de mandat",
    "Année de fin de mandat",
    "Motif fin de mandat",
    "Commentaire",
]


def crawl_row(
    context: Context,
    row: Dict[str, str],
    mandate_dict: Dict[str, List[Tuple[Optional[str], Optional[str]]]],
):
    """Process one row of the CSV data"""
    # Unique senator ID (note: *not* a national ID number)
    senid = row.pop("Matricule")
    status = row.pop("État")
    prefix = row.pop("Qualité")
    family_name = row.pop("Nom usuel")
    given_name = row.pop("Prénom usuel")
    birth_date = row.pop("Date naissance")
    death_date = row.pop("Date de décès")
    email = row.pop("Courrier électronique")
    context.log.debug(
        f"Senator {senid}: {prefix} {given_name} {family_name} ({status})"
    )
    # Ignore various other fields
    context.audit_data(row, UNUSED_FIELDS)
    # Make a PEP entity
    person = context.make("Person")
    person.id = context.make_slug(senid)
    context.log.debug(f"Unique ID {person.id}")
    h.apply_name(
        person, prefix=prefix, given_name=given_name, last_name=family_name, lang="fra"
    )
    if email and email != "Non public":
        person.add("email", email)
    if birth_date:
        # Luckily they are all consistently formatted!
        person.add("birthDate", birth_date)
    if death_date:
        person.add("deathDate", death_date)
    position = h.make_position(
        context,
        name="Senator of the French Fifth Republic",
        country="fr",
        topics=["gov.national", "gov.legislative"],
    )
    # is_pep=True because we expect all senators to be PEPs
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        context.log.warning(f"Senator {given_name} {family_name} is not PEP")
        return
    # Start dates seem to be missing for senators elected in 2023, so
    # we will need to add an occupancy for current senators even if
    # they don't have any in the mandates file.
    #
    # When only adding these empty mandates to those missing mandates,
    # we only got 252 senators with current occupancy, so let's add it to all
    # with ACTIF, then we at least get 347 currents as of writing.
    mandates = mandate_dict.get(senid, [])
    if status == "ACTIF":
        mandates.append((None, None))
    entities: List[Entity] = []
    for start_date, end_date in mandates:
        context.log.debug(f"Mandate for {senid}: {start_date} - {end_date}")
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=True,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
        )
        if occupancy is not None:
            entities.append(occupancy)
    if entities:
        context.log.debug(f"Emitting PEP entities for {senid}")
        context.emit(person, target=True)
        context.emit(position)
        for entity in entities:
            context.emit(entity)


def crawl_mandates(
    context: Context, reader: Iterator[Dict[str, str]]
) -> Iterator[Tuple[str, Tuple[Optional[str], Optional[str]]]]:
    """Get mandates for senators by ID."""
    for row in reader:
        senid = row.pop("Matricule")
        start_date = row.pop("Date de début de mandat") or None
        if start_date is not None:
            start_date = start_date.split(maxsplit=1)[0]
        end_date = row.pop("Date de fin de mandat") or None
        if end_date is not None:
            end_date = end_date.split(maxsplit=1)[0]
        context.audit_data(row, UNUSED_MANDATE_FIELDS)
        yield senid, (start_date, end_date)


def crawl(context: Context):
    """Retrieve list of senators as CSV and emit PEP entities for
    currently serving senators."""
    # Get start dates from auxiliary CSV
    path = context.fetch_resource(
        "mandates.csv", urljoin(context.data_url, "/data/senateurs/ODSEN_ELUSEN.csv")
    )
    mandates: Dict[str, List[Tuple[Optional[str], Optional[str]]]] = {}
    with open(path, "rt", encoding="cp1252") as infh:
        decomment = (spam for spam in infh if spam[0] != "%")
        for senid, dates in crawl_mandates(context, csv.DictReader(decomment)):
            mandates.setdefault(senid, []).append(dates)

    # Do main CSV
    path = context.fetch_resource("senators.csv", context.data_url)
    with open(path, "rt", encoding="cp1252") as infh:
        decomment = (spam for spam in infh if spam[0] != "%")
        for row in csv.DictReader(decomment):
            crawl_row(context, row, mandates)
