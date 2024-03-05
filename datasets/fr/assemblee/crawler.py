"""
Crawler for members of the French National Assembly.
"""

import json
import re
from typing import Dict, Any
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

REGEX_PATH = re.compile(r"^json/acteur/")


def crawl_acteur(context, data: Dict[str, Any]):
    """Extract MNA information from JSON."""
    acteur = data.pop("acteur")
    context.audit_data(data, ["@xmlns", "profession"])

    person = context.make("Person")
    # Member UID
    uid = acteur.pop("uid")
    person.id = context.make_slug(uid.pop("#text"))
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
    for mandat in mandats:
        if mandat.pop("typeOrgane") == "ASSEMBLEE":
            start_date = mandat.pop("dateDebut")
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
