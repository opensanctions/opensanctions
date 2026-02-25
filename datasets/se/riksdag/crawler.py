from typing import Optional, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


# The dataset includes both full members and temporary substitutes ("ersättare") but
# does NOT always provide explicit parliamentary term boundaries for every person.


def translate_keys(context: Context, member_dict: dict[str, Any]) -> dict[str, Any]:
    # Translate top-level keys
    return {context.lookup_value("keys", k) or k: v for k, v in member_dict.items()}


def extract_terms(
    item: dict[str, Any],
) -> tuple[Optional[str], Optional[str]]:
    """
    Extract the start and end dates of the main parliamentary term.
    """
    # Data model provides all roles a person has ever had, including roles in
    # committees, government agencies, political party functions, or temporary
    # substitute assignments. We are only interested in roles that represent
    # actual service in the national parliament
    #
    #    organ_kod == "kam"             assignment belongs to the parliamentary chamber
    #    typ == "kammaruppdrag"         chamber-level duty (not committee work)
    for role in item.pop("person_mandate", {}).pop("mandate", []):
        if role.get("organ_kod") == "kam" and role.get("typ") == "kammaruppdrag":
            if role.get("roll_kod") in {
                "Ersättare",  #  Substitute MP
                "Riksdagsledamot",  # MP
                "Statsrådsersättare",  # MP substituting for a minister
            }:
                start = role.get("from")
                end = role.get("tom")
                return start, end
    return None, None


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

        # They only use abbreviations, which are pretty meaningless.
        # Full names only on their website.
        party_key = item.pop("party")
        party = context.lookup_value("parties", party_key, warn_unmatched=True)
        entity.add("political", party)

        entity.add("citizenship", "se")
        entity.add("gender", item.pop("gender"))

        position = h.make_position(
            context,
            name="Member of the Swedish Riksdag",
            wikidata_id="Q10655178",
            country="se",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        start_date, end_date = extract_terms(item)
        # We only scrape MPs from 2018 onward. Some individuals lack explicit
        # parliamentary term dates; in those cases, the status is set to UNKNOWN.
        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
            no_end_implies_current=False,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(entity)

        context.audit_data(
            item,
            [
                "constituency",
                "hangar_guid",
                "sourceid",
                "hangar_id",
                "sort_name",
                "bild_url_80",
                "bild_url_192",
                "bild_url_max",
                "person_info",
                "iort",
                "status",
                "person_url_xml",
            ],
        )
