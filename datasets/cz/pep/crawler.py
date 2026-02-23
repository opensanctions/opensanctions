from datetime import datetime
from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, get_after_office

JUDGE_OVM_ID = "a6b0a81a-cf8a-4319-8fd1-5d509d5ce1df"
# JUDGE_POSITION_ID = "f2a7d68f-d38f-4c3d-a820-919ec5924f4a"


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
        # position_full_name = wp.get("name")
        # org_name = wp.get("organization")

        wp_data = wp.get("workingPosition")
        # Some persons hold a deputy or senator role alongside other roles.
        # The API flags this with deputyAndOthers / senatorAndOthers at the
        # person level. We only look at the position-level deputy/senator flags
        # here, since the deputy/senator position will be emitted on its own
        # working position entry anyway.
        is_deputy = wp_data.get("deputy")
        is_senator = wp_data.get("senator")
        is_judge = item.get("judge")
        # ovmId identifies the type of position, shared across all holders of that role
        ovm_id = wp.get("ovmId")
        # position_id = wp_data.get("id")
        position_name = wp.get("name")
        # position_name = wp_data.get("name")

        # judge is a person-level flag, not position-level, so a judge person
        # will have it set on ALL their working positions, including non-judicial
        # ones. We narrow to the single position_id that represents the judge role.
        if is_judge and ovm_id != JUDGE_OVM_ID:
            continue

        if not (is_deputy or is_senator or is_judge):
            continue
        # Judge ovm_id also includes prosecutors, so we categorize tham using the
        # position_name lookup instead.
        lookup = "judicial_details" if is_judge else "position_details"
        lookup_value = position_name if is_judge else ovm_id
        res = context.lookup(lookup, lookup_value, warn_unmatched=True)
        if not res or not res.items:
            continue

        position = h.make_position(
            context,
            name=res.items["name"],
            wikidata_id=res.items["qid"],
            country="cz",
            topics=res.items["topics"],
            lang="eng",
        )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        end_date = wp.get("end")  # alternative key: writtenDateOfEnd
        if end_date is not None:
            cutoff_date = datetime.now() - get_after_office(position.get("topics"))
            if datetime.strptime(end_date, "%Y-%m-%d") < cutoff_date:
                context.log.info(f"Skipping old term (ended {end_date})")
                continue

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=wp.get("start"),
            end_date=end_date,
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
