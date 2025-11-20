from typing import Optional, Dict, Any, Tuple

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


# The Riksdag Open Data API endpoint we use as a data_url (see docs: https://www.riksdagen.se/sv/dokument-och-lagar/riksdagens-oppna-data/)
# returns only individuals who have served in the Swedish Parliament in some capacity since 2018.
# The dataset includes both full members and temporary substitutes ("ersättare") but does NOT always provide
# explicit parliamentary term boundaries for every person.


def translate_keys(member, context) -> dict:
    # Translate top-level keys
    translated = {context.lookup_value("keys", k) or k: v for k, v in member.items()}
    return translated


def extract_terms(
    item: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract the start and end dates of the main parliamentary term.
    """
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


def extract_gender(context, gender, entity_id) -> Optional[str]:
    # Okänt is Swedish for "unknown"
    if "okänt" in gender.lower():
        return None
    gender = context.lookup_value("gender", gender)
    if not gender:
        context.log.warning(f"Unknown gender for {entity_id}")
    return gender


def crawl(context: Context):
    data = context.fetch_json(context.data_url, cache_days=3)
    for item in data["personlista"]["person"]:
        item = translate_keys(item, context)
        id = item.pop("official_id")
        first_name = item.pop("first_name")
        last_name = item.pop("last_name")

        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, id)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        h.apply_date(entity, "birthDate", item.pop("birth_year"))
        entity.add("political", item.pop("party"))
        entity.add("citizenship", "se")
        entity.add("sourceUrl", item.pop("person_url_xml"))
        entity.add("gender", extract_gender(context, item.pop("gender"), entity.id))

        position = h.make_position(
            context,
            name="Member of the Swedish Rikstag",
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
            ],
        )
