from datetime import datetime
from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, get_after_office

POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_DATE = datetime.now() - get_after_office(POSITION_TOPICS)


link = "https://cro.justice.cz/verejnost/api/funkcionari/68219d51-998d-49bf-9fb3-77563880e196"
ovmId = "https://cro.justice.cz/verejnost/api/funkcionari/af9a98a7-ac7a-4c8a-922d-7c169328a897"

jud_topics = ["gov.judicial", "gov.national"]
leg_topics = ["gov.legislative", "gov.national"]
exe_topics = ["gov.executive", "gov.national"]


def crawl_person(context: Context, item: Dict[str, Any]) -> None:
    person_id = item.get("id")
    first_name = item.get("firstName")
    last_name = item.get("lastName")

    entity = context.make("Person")
    entity.id = context.make_id(person_id)
    h.apply_name(
        entity,
        first_name=first_name,
        last_name=last_name,
    )
    entity.add(
        "sourceUrl",
        f"https://cro.justice.cz/verejnost/api/funkcionari/{person_id}",
    )
    entity.add("title", item.get("titleBefore"))
    entity.add("citizenship", "cz")

    for wp in item.get("workingPositions", []):
        # position = h.make_position(context, wp.get("name"))
        # deputyAndOthers / senatorAndOthers mean the person holds one of the
        # primary roles PLUS additional roles — still qualifies.
        wp_data = wp.get("workingPosition")
        is_deputy = wp_data.get("deputy") or wp_data.get("deputyAndOthers")
        # is_senator = wp.get("senator") or item.get("senatorAndOthers")
        # is_judge = wp.get("judge")
        # ovmID is a unique identifier for a specific position
        ovmId = wp.get("ovmId")

        if not is_deputy:  #  or is_senator or is_judge):
            return
        print(wp.get("name"))
        # uni
        print(ovmId)
        res = context.lookup("position_details", ovmId, warn_unmatched=True)
        if res and res.items:
            name = res.items["name"]
            qid = res.items["qid"]
            topics = res.items["topics"]

        position = h.make_position(
            context,
            name=name,
            wikidata_id=qid,
            country="cz",
            topics=topics,
            lang="eng",
        )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=wp.get("start"),
            end_date=wp.get("end"),
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(entity)
            context.emit(position)
            context.emit(occupancy)


def crawl(context: Context) -> None:
    page = 0
    while True:
        url = f"{context.data_url}?sort=created&order=DESC&page={page}&pageSize=100"
        # url = "https://cro.justice.cz/verejnost/api/funkcionari/68219d51-998d-49bf-9fb3-77563880e196"
        data = context.fetch_json(url, cache_days=7)
        if data is None:
            context.log.error("Empty response", url=url)
            break

        items = data.get("items")
        if not items:
            break

        for item in items:
            crawl_person(context, item)

        page += 1
        if page > 200:
            context.log.info("hit max pages")
            break
