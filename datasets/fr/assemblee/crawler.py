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


def is_not_nil(value: Any) -> bool:
    """Null values could be `null` (parsed to None by JSON) or they
    could be @xml:junk in a dictionary."""
    try:
        if value is None:
            return False
        if value.get("@xsi:nil") == "true":
            return False
    except AttributeError:
        # It wasn't a dictionary and it wasn't None so it's not nil
        pass
    return True


def crawl_collabos(
    context, person: Entity, uid: str, mandat: Dict[str, Any]
) -> Iterator[Entity]:
    """Add staff (parliamentry collaborators) as associates."""
    collabos = mandat.pop("collaborateurs")
    if collabos is None:
        return
    collabos = collabos.get("collaborateur")
    if collabos is None:
        return
    if not isinstance(collabos, list):
        collabos = [collabos]
    for i, c in enumerate(collabos):
        collabo = context.make("Person")
        prefix = c.pop("qualite")
        first_name = c.pop("prenom")
        last_name = c.pop("nom")
        collabo.id = context.make_slug(uid, "collabo", prefix, first_name, last_name)
        h.apply_name(
            collabo,
            prefix=prefix,
            first_name=first_name,
            last_name=last_name,
        )
        collabo.set("topics", "role.rca")
        yield collabo

        link = context.make("Associate")
        link.id = context.make_slug(uid, f"associate", prefix, first_name, last_name)
        link.set("person", person)
        link.set("associate", collabo)
        link.set("relationship", "collaborateur")
        start_date = c.pop("dateDebut")
        if is_not_nil(start_date):
            link.set("startDate", start_date)
        end_date = c.pop("dateFin")
        if is_not_nil(end_date):
            link.set("endDate", end_date)
        yield link
        context.audit_data(c)


def crawl_acteur(context, data: Dict[str, Any]):
    """Extract MNA information from JSON."""
    acteur = data.pop("acteur")
    context.audit_data(data, ["@xmlns", "profession"])

    person = context.make("Person")
    # Member UID
    uid = acteur.pop("uid")
    uid_text = uid.pop("#text")
    context.log.debug(f"Unique ID is {uid_text}")
    person.id = context.make_slug(uid_text)
    context.audit_data(uid, ["@xmlns:xsi", "@xsi:type"])

    # Name DOB, POB, DOD (or DØD if you're Danish)
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
    birth = ec.pop("infoNaissance")
    dob = birth.pop("dateNais")
    if is_not_nil(dob):
        person.set("birthDate", h.parse_date(dob, ["%Y-%m-%d"]))
    city = birth.pop("villeNais")
    if is_not_nil(city):
        person.set("birthPlace", city)
    country = birth.pop("paysNais")
    if is_not_nil(country):
        person.set("birthCountry", country)
    # Ignore birth departement for now
    context.audit_data(birth, ["depNais"])
    dod = ec.pop("dateDeces")
    if is_not_nil(dod):
        person.set("deathDate", h.parse_date(dod, ["%Y-%m-%d"]))
    context.audit_data(ec)

    # We'll include this URL in the data as it's quite useful
    hatvp = acteur.pop("uri_hatvp")
    if is_not_nil(hatvp):
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
    # Occupancy data (there are many mandats but we only care about
    # ASSEMBLEE).
    mandats = acteur.pop("mandats")
    if mandats is None:
        context.log.warning(f"No mandats found for {uid_text}")
    mandats = mandats.get("mandat")
    if mandats is None:
        context.log.warning(f"No mandats found for {uid_text}")
    if not isinstance(mandats, list):
        mandats = [mandats]
    start_date = None
    entities: List[Entity] = []
    for mandat in mandats:
        if mandat.pop("typeOrgane") == "ASSEMBLEE":
            start_date = mandat.pop("dateDebut")
            end_date = mandat.pop("dateFin")
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                True,
                start_date=start_date,
                end_date=end_date,
                categorisation=categorisation,
            )
            if occupancy is not None:
                entities.append(occupancy)
                # Parliamentary collaborators (i.e. staff)
                entities.extend(crawl_collabos(context, acteur, uid_text, mandat))
    if entities:
        context.log.debug(f"Emitting PEP entities for {uid_text}")
        context.emit(person, target=True)
        context.emit(position)
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
