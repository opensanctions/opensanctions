from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


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
    entity.add("title", item.get("titleBefore"))
    entity.add("title", item.get("titleAfter"))
    entity.add("citizenship", "cz")
    # is_judge is a person-level flag
    is_judge = item.get("judge")
    for wp in item.get("workingPositions", []):
        wp_data = wp.get("workingPosition")
        # Some persons hold a deputy or senator role alongside other roles.
        # The API flags this with deputyAndOthers / senatorAndOthers at the
        # person level. We only look at the position-level deputy/senator flags
        # here, since the deputy/senator position will be emitted on its own
        # working position entry anyway.
        is_deputy = wp_data.get("deputy")
        is_senator = wp_data.get("senator")
        _position_name = wp.get("name")

        if is_deputy or is_senator or is_judge:
            is_pep = True
        else:
            is_pep = None

        role_name = wp_data.get("name")
        org_name = wp.get("organization")
        position_name = f"{role_name}, {org_name}" if org_name else role_name
        position = h.make_position(
            context,
            # We construct a clean position_name without the date at the end:
            # e.g. člen statutárního orgánu Kulturní zařízení města Přibyslav 01.02.2026
            # -> člen statutárního orgánu Kulturní zařízení města Přibyslav
            name=position_name,
            country="cz",
            lang="ces",
        )
        entity.add("position", position_name)

        # We mark them all as PEPs by the source definition
        categorisation = categorise(context, position, is_pep=is_pep)
        if not categorisation.is_pep:
            continue

        # alternative key: writtenDateOfEnd
        end_date = wp.get("end") or wp.get("writtenDateOfEnd")
        if wp.get("end") and wp.get("writtenDateOfEnd"):
            end_date = max(wp.get("end"), wp.get("writtenDateOfEnd"))

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=wp.get("start"),
            end_date=end_date,
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(
                entity,
                origin=f"https://cro.justice.cz/verejnost/api/funkcionari/{person_id}",
            )
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
