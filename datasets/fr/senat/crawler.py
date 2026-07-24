"""
Crawler for active French senators.
"""

import csv
from collections import defaultdict
from typing import NamedTuple
from collections.abc import Iterator
from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import categorise


class Mandate(NamedTuple):
    start_date: str | None
    end_date: str | None
    election_date: str | None


UNUSED_FIELDS = [
    "Type d'app au grp politique",
    "Commission permanente",
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
    "Année de début de mandat",
    "Motif début de mandat",
    "Année de fin de mandat",
    "Motif fin de mandat",
    "Commentaire",
]


def crawl_row(
    context: Context,
    row: dict[str, str],
    mandates_by_senid: dict[str, list[Mandate]],
) -> None:
    # Unique senator ID (note: *not* a national ID number)
    senid = row.pop("Matricule")
    status = row.pop("État")
    prefix = row.pop("Qualité")
    family_name = row.pop("Nom usuel")
    given_name = row.pop("Prénom usuel")
    birth_date = row.pop("Date naissance")
    death_date = row.pop("Date de décès")
    email = row.pop("Courrier électronique")
    constituency = row.pop("Circonscription")
    political_group = row.pop("Groupe politique")
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
    # citizenship required: https://www.elections.interieur.gouv.fr/scrutins/elections-senatoriales/elections-senatoriales-je-suis-candidat
    person.add("citizenship", "fr")
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
    categorisation = categorise(context, position, default_is_pep=True)
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
    mandates = mandates_by_senid.get(senid, [])
    if status == "ACTIF":
        mandates.append(Mandate(start_date=None, end_date=None, election_date=None))
    entities: list[Entity] = []
    for start_date, end_date, election_date in mandates:
        context.log.debug(f"Mandate for {senid}: {start_date} - {end_date}")
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date,
            end_date=end_date,
            election_date=election_date,
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", constituency)
        occupancy.add("politicalGroup", political_group)
        entities.append(occupancy)
    if entities:
        context.log.debug(f"Emitting PEP entities for {senid}")
        context.emit(person)
        context.emit(position)
        for entity in entities:
            context.emit(entity)


def crawl_mandates(
    context: Context, reader: Iterator[dict[str, str]]
) -> Iterator[tuple[str, Mandate]]:
    """Get mandates for senators by ID."""
    for row in reader:
        senid = row.pop("Matricule")
        start_date = row.pop("Date de début de mandat") or None
        if start_date is not None:
            start_date = start_date.split(maxsplit=1)[0]
        end_date = row.pop("Date de fin de mandat") or None
        if end_date is not None:
            end_date = end_date.split(maxsplit=1)[0]
        election_date = row.pop("Date d'élection") or None
        if election_date is not None:
            election_date = election_date.split(maxsplit=1)[0]
        context.audit_data(row, UNUSED_MANDATE_FIELDS)
        yield (
            senid,
            Mandate(
                start_date=start_date, end_date=end_date, election_date=election_date
            ),
        )


def crawl(context: Context) -> None:
    """Retrieve list of senators as CSV and emit PEP entities for
    currently serving senators."""
    # Get start dates from auxiliary CSV
    path = context.fetch_resource(
        "mandates.csv", urljoin(context.data_url, "/data/senateurs/ODSEN_ELUSEN.csv")
    )
    # Keyed by senator matricule (senid); one senator can have multiple mandates
    # covering different terms, each with its own start/end/election dates.
    mandates_by_senid: defaultdict[str, list[Mandate]] = defaultdict(list)
    with open(path, encoding="cp1252") as infh:
        decomment = (spam for spam in infh if spam[0] != "%")
        for senid, mandate in crawl_mandates(context, csv.DictReader(decomment)):
            mandates_by_senid[senid].append(mandate)

    # Main CSV: one row per senator with biographical and status fields
    path = context.fetch_resource("senators.csv", context.data_url)
    with open(path, encoding="cp1252") as infh:
        decomment = (spam for spam in infh if spam[0] != "%")
        for row in csv.DictReader(decomment):
            crawl_row(context, row, mandates_by_senid)
