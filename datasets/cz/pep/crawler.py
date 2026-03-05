from datetime import datetime
from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, get_after_office

JUDGE_OVM_ID = "a6b0a81a-cf8a-4319-8fd1-5d509d5ce1df"
# JUDGE_POSITION_ID = "f2a7d68f-d38f-4c3d-a820-919ec5924f4a"

MINISTER_ID = "8c7d612b-dabd-4c20-89a6-6c6a5d26e87e"
MINISTER_OVM_ID = "f2986792-0539-44d8-ae4a-30faba776b36"
# 98450ad9-576a-4f35-a511-dbe06f3f5db4 reditel odboru
# a23ba8b7-06ca-42a5-afef-c04fc45897e7 namestek clean vlady


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
    entity.add("title", item.get("titleAfter"))
    entity.add("citizenship", "cz")
    for wp in item.get("workingPositions", []):
        wp_data = wp.get("workingPosition")
        # Some persons hold a deputy or senator role alongside other roles.
        # The API flags this with deputyAndOthers / senatorAndOthers at the
        # person level. We only look at the position-level deputy/senator flags
        # here, since the deputy/senator position will be emitted on its own
        # working position entry anyway.
        is_deputy = wp_data.get("deputy")
        is_senator = wp_data.get("senator")
        is_judge = item.get("judge")
        ovm_id = wp.get("ovmId")
        position_name = wp.get("name")
        res = None

        # judge is a person-level flag, not position-level, so a judge person
        # will have it set on ALL their working positions, including non-judicial
        # ones. We narrow to the single position_id that represents the judge role.
        if is_judge and ovm_id == JUDGE_OVM_ID:
            res = context.lookup("judicial_details", position_name, warn_unmatched=True)
        elif is_deputy or is_senator or ovm_id == MINISTER_OVM_ID:
            res = context.lookup("position_details", ovm_id, warn_unmatched=True)
        else:
            res = context.lookup("position_details", position_name)

        if res and res.items:
            position = h.make_position(
                context,
                name=res.items["name"],  # fix: index into list
                wikidata_id=res.items["qid"],
                country="cz",
                topics=res.items["topics"],
                lang="eng",
            )
        else:
            # Fallback: construct position from API fields
            role_name = wp_data.get("name")
            org_name = wp.get("organization")
            position = h.make_position(
                context,
                name=f"{role_name}, {org_name}" if org_name else role_name,
                country="cz",
                lang="ces",
            )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        end_date = wp.get("end")  # alternative key: writtenDateOfEnd
        if end_date is not None:
            cutoff = datetime.now() - get_after_office(position.get("topics"))
            if datetime.strptime(end_date, "%Y-%m-%d") < cutoff:
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
