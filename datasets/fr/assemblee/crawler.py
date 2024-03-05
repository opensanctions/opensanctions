"""
Crawler for members of the French National Assembly.
"""

import json
import re
from typing import Any, Dict, Iterator, List
from zipfile import ZipFile

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise

REGEX_PATH = re.compile(r"^json/acteur/")


def crawl_collabos(
    context, person: Entity, uid: str, mandat: Dict[str, Any]
) -> Iterator[Entity]:
    """Add staff (parliamentry collaborators) as associates."""
    collabos = mandat.pop("collaborateurs")["collaborateur"]
    if not isinstance(collabos, list):
        collabos = [collabos]
    for i, c in enumerate(collabos):
        collabo = context.make("Person")
        collabo.id = context.make_slug(uid, f"collabo{i}")
        h.apply_name(
            collabo,
            prefix=c.pop("qualite"),
            first_name=c.pop("prenom"),
            last_name=c.pop("nom"),
        )
        collabo.set("topics", "role.rca")
        context.audit_data(c, ["dateDebut", "dateFin"])
        yield collabo

        link = context.make("Associate")
        link.id = context.make_slug(uid, f"associate-collabo{i}")
        link.set("person", person)
        link.set("associate", collabo)
        link.set("relationship", "collaborateur")
        yield link


def crawl_acteur(context, data: Dict[str, Any]):
    """Extract MNA information from JSON."""
    acteur = data.pop("acteur")
    context.audit_data(data, ["@xmlns", "profession"])

    person = context.make("Person")
    # Member UID
    uid = acteur.pop("uid")
    uid_text = uid.pop("#text")
    person.id = context.make_slug(uid_text)
    context.audit_data(uid, ["@xmlns:xsi", "@xsi:type"])

    # Name and DOB
    ec = acteur.pop("etatCivil")
    ident = ec.pop("ident")
    h.apply_name(
        person,
        prefix=ident.pop("civ"),
        first_name=ident.pop("prenom"),
        last_name=ident.pop("nom"),
        lang="fra",
    )
    context.audit_data(ident, ["alpha", "trigramme"])
    dob = ec.pop("infoNaissance")
    person.set("birthDate", h.parse_date(dob.pop("dateNais"), ["%Y-%m-%d"]))
    person.set("birthPlace", dob.pop("villeNais"))
    person.set("birthCountry", dob.pop("paysNais"))
    # Ignore birth departement for now
    context.audit_data(dob, ["depNais"])
    # Assume current MNAs are not dead
    context.audit_data(ec, "dateDeces")

    # We'll include this URL in the data as it's quite useful
    hatvp = acteur.pop("uri_hatvp")
    person.set("sourceUrl", hatvp)

    # Addresses and phone numbers are available because transparence!
    # But we do not include them because vie privée!
    acteur.pop("adresses")

    # Now make position etc
    position = h.make_position(
        context,
        "member of the French National Assembly",
        country="fr",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return
    # Occupancy data (there are many mandats but we only care about AN)
    mandats = acteur.pop("mandats")["mandat"]
    start_date = None
    entities: List[Entity] = []
    for mandat in mandats:
        if mandat.pop("typeOrgane") == "ASSEMBLEE":
            start_date = mandat.pop("dateDebut")
            # Parliamentary collaborators (i.e. staff)
            entities.extend(crawl_collabos(context, acteur, uid_text, mandat))
            break
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        True,
        start_date=start_date,
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)
        for entity in entities:
            context.emit(entity)


def crawl(context: Context):
    """Download the database of MNAs and create PEP entities."""
    path = context.fetch_resource("deputes.zip", context.data_url)
    with ZipFile(path) as archive:
        for member in archive.infolist():
            if REGEX_PATH.match(member.filename) is None:
                continue
            context.log.debug(f"Extracting member from {member.filename}")
            with archive.open(member) as fh:
                crawl_acteur(context, json.load(fh))
