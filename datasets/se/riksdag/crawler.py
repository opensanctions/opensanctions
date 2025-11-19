from zavod import Context, helpers as h
from zavod.stateful.positions import OccupancyStatus, categorise


def translate_keys(member, context) -> dict:
    # Translate top-level keys
    translated = {context.lookup_value("keys", k) or k: v for k, v in member.items()}
    # Translate all nested dicts at level 2
    for k, v in translated.items():
        if isinstance(v, dict):
            translated[k] = {
                context.lookup_value("keys", nk) or nk: nv for nk, nv in v.items()
            }
    return translated


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
        entity.add(
            "gender", context.lookup_value("gender", item.pop("gender")), lang="eng"
        )
        if not entity.get("gender")[0]:
            context.log.warning(f"Unknown gender for {entity.id}")

        position = h.make_position(
            context,
            name="Member of the Swedish Rikstag",
            wikidata_id="Q10655178",
            country="se",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )
        status = item.pop("status")
        is_pep = True if "tjänstgörande riksdagsledamot" in status.lower() else False

        categorisation = categorise(context, position, is_pep=is_pep)
        if not categorisation.is_pep:
            return

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            # start_date=term.split("-")[0],
            # end_date=term.split("-")[1],
            categorisation=categorisation,
            status=OccupancyStatus.CURRENT,
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
                "person_mandate",
                "person_info",
                "iort",
            ],
        )
