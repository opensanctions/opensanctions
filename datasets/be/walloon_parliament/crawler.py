import json
from typing import Any, Dict
from rigour.mime.types import JSON

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

IGNORE = [
    "dep_adresse_postale",
    "dep_circons",  # electoral constituency
    "dep_email",
    "dep_fax",
    "dep_fonction_bureau_elargi",
    "dep_fonction_bureau",
    "dep_fonction_conf_pres",
    "dep_membre_bureau_elargi",
    "dep_membre_bureau",
    "dep_membre_conf_pres",
    "dep_ordre_bureau_elargi",
    "dep_ordre_bureau",
    "dep_ordre_conf_pres",
    "dep_photo",
    "dep_presidentgroupe",  # group president (yes/no)
    "dep_province",
    "dep_siege",  # seat number
    "dep_social_nt",
    "dep_tel",
]


def crawl_item(context: Context, item: Dict[str, Any]) -> None:
    dep_id = item.pop("dep_id")
    full_name = item.pop("dep_nomcomplet")

    entity = context.make("Person")
    entity.id = context.make_id(dep_id, full_name)
    h.apply_name(
        entity,
        full=full_name,
        first_name=item.pop("dep_prenom"),
        last_name=item.pop("dep_nom"),
    )
    entity.add("gender", item.pop("dep_civ"))
    entity.add("political", item.pop("dep_parti"))
    entity.add("website", item.pop("dep_url"))
    entity.add("citizenship", "be")

    position = h.make_position(
        context,
        name="Member of the Parliament of Wallonia",
        wikidata_id="Q19351455",
        country="be",
        topics=["gov.legislative", "gov.state"],
        lang="eng",
    )
    position.add("subnationalArea", item.pop("dep_province"))

    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context, person=entity, position=position, categorisation=categorisation
    )

    if occupancy is not None:
        # plenary session mandate type
        occupancy.add("description", item.pop("dep_mandat_seance_pleniere"))
        context.emit(entity)
        context.emit(occupancy)
        context.emit(position)

    context.audit_data(item, ignore=IGNORE)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for item in data["deputes"]:
        crawl_item(context, item)
