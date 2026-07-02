from typing import Any, Optional

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise


# Chamber roles that constitute actual membership of the Riksdag. Committee work
# and other duties are listed under separate organ codes and mandate types.
RIKSDAG_ROLES = {
    "Ersättare",  # Substitute MP
    "Riksdagsledamot",  # MP
    "Statsrådsersättare",  # MP substituting for a minister
}

# Mandate types that are sub-roles of holding office rather than offices in their
# own right: committee seats, parliamentary bodies/delegations, party group
# functions, and speaker duties. Speakers and party group leaders are themselves
# sitting MPs, so they are already captured via their chamber membership.
SKIP_MANDATE_TYPES = {
    "uppdrag",  # committee assignments
    "Riksdagsorgan",  # parliamentary bodies and international delegations
    "partiuppdrag",  # party group functions (leader, secretary, ...)
    "talmansuppdrag",  # speaker and deputy speakers (also sitting MPs)
}


def translate_keys(context: Context, member_dict: dict[str, Any]) -> dict[str, Any]:
    # Translate top-level keys
    return {context.lookup_value("keys", k) or k: v for k, v in member_dict.items()}


def extract_biography(person_info: Any) -> Optional[str]:
    """Join the source's biographical text blocks into a single narrative.

    The `personuppgift` payload mixes biography with images, contact details and
    election flags; only the `biografi`-typed entries are biographical prose.
    Each is labelled with a Swedish heading (e.g. "Utbildning", "Föräldrar"),
    which we keep verbatim to preserve the structure of the source.
    """
    if not isinstance(person_info, dict):
        return None
    lines: list[str] = []
    for entry in person_info.get("uppgift", []):
        if entry.get("typ") != "biografi":
            continue
        body = " ".join(t for t in entry.get("uppgift", []) if t).strip()
        if not body:
            continue
        heading = entry.get("kod")
        lines.append(f"{heading}: {body}" if heading else body)
    return "\n".join(lines) if lines else None


def position_for_mandate(context: Context, role: dict[str, Any]) -> Optional[Entity]:
    """Build the Position a single mandate entry corresponds to, or None to skip it.

    The source lists every assignment a person has ever held. Only a few of them
    are public offices we track as PEP positions: national chamber membership,
    European Parliament membership, and cabinet posts. Committee seats,
    delegations and party functions are skipped as internal sub-roles.
    """
    typ = role.get("typ")
    roll = role.get("roll_kod")
    if typ == "kammaruppdrag":
        if roll in RIKSDAG_ROLES:
            return h.make_position(
                context,
                name="Member of the Swedish Riksdag",
                wikidata_id="Q10655178",
                country="se",
                topics=["gov.legislative", "gov.national"],
                lang="eng",
            )
        return None
    if typ == "Europaparlamentet" and roll == "Ledamot":
        return h.make_position(
            context,
            name="Member of the European Parliament",
            country="eu",
            topics=["gov.igo", "gov.legislative"],
            lang="eng",
        )
    if typ == "Departement":
        # Ministers are listed with their Swedish title (e.g. "Statsråd",
        # "Finansminister", "Statsminister").
        if not roll:
            context.log.warning("Cabinet mandate without a title", role=role)
            return None
        return h.make_position(
            context,
            name=roll,
            country="se",
            topics=["gov.national", "gov.executive"],
            lang="swe",
            translate_name=True,
        )
    if typ in SKIP_MANDATE_TYPES:
        return None
    context.log.warning("Unexpected mandate type", typ=typ, roll=roll)
    return None


def crawl(context: Context) -> None:
    data = context.fetch_json(
        context.data_url,
        params={
            "utformat": "json",
            # rdlstatus="" gives only active members
            # rdlstatus="tjanst" (in service, for whatever reason) gives all members of the current and past term
            # rdlstatus="samtliga" (all) gives members of all terms (since 1991)
            # We pass "tjanst" because some members don't include term dates in their data,
            # so we can't reliably filter the really old ones from "samtliga".
            "rdlstatus": "tjanst",
        },
        cache_days=3,
    )
    for item in data["personlista"]["person"]:
        item = translate_keys(context, item)
        id = item.pop("official_id")
        first_name = item.pop("first_name")
        last_name = item.pop("last_name")

        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, id)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        h.apply_date(entity, "birthDate", item.pop("birth_year"))
        entity.add("citizenship", "se")
        entity.add("gender", item.pop("gender"))

        # They only use abbreviations, which are pretty meaningless.
        # Full names only on their website.
        party_key = item.pop("party")
        party = context.lookup_value("parties", party_key, warn_unmatched=True)
        entity.add("political", party)

        entity.add("biography", extract_biography(item.pop("person_info", None)))

        # Electoral district the member represents; only meaningful for their
        # chamber seat, so it is attached to the Riksdag occupancy below.
        constituency = item.pop("constituency", None)

        # A member with no registered assignments has an empty string here instead
        # of a {"uppdrag": [...]} object, so we can't assume a dict.
        person_mandate = item.pop("person_mandate", None)
        mandates = (
            person_mandate.get("uppdrag") if isinstance(person_mandate, dict) else None
        )
        if not isinstance(mandates, list):
            mandates = []

        # Emit one occupancy per qualifying mandate (a person may hold several
        # terms, or sit in both the Riksdag and a cabinet). make_occupancy drops
        # occupancies whose term ended beyond the position's after-office window,
        # so historical-only members fall away here.
        emitted = False
        for role in mandates:
            position = position_for_mandate(context, role)
            if position is None:
                continue
            categorisation = categorise(context, position, default_is_pep=True)
            if not categorisation.is_pep:
                continue
            occupancy = h.make_occupancy(
                context,
                person=entity,
                position=position,
                start_date=role.get("from"),
                end_date=role.get("tom"),
                categorisation=categorisation,
                # Citizenship is set explicitly above; don't let an MEP seat
                # (country "eu") propagate onto the person as a country.
            )
            if occupancy is not None:
                if role.get("typ") == "kammaruppdrag":
                    occupancy.add("constituency", constituency)
                context.emit(position)
                context.emit(occupancy)
                emitted = True
        if emitted:
            context.emit(entity)

        context.audit_data(
            item,
            [
                "hangar_guid",
                "sourceid",
                "hangar_id",
                "sort_name",
                "bild_url_80",
                "bild_url_192",
                "bild_url_max",
                "iort",
                "status",
                "person_url_xml",
            ],
        )
