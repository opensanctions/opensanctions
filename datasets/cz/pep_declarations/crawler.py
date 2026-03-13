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
        # is_deputy and is_senator are position-level flags
        is_deputy = wp_data.get("deputy")
        is_senator = wp_data.get("senator")
        # Only the main institutions are marked as PEP by default
        if is_deputy or is_senator or is_judge:
            is_pep = True
        else:
            is_pep = None

        role_name = wp_data.get("name")
        org_name = wp.get("organization")
        # Construct a clean position_name:
        # e.g. poslanec, Kancelář Poslanecké sněmovny Parlamentu
        position_name = f"{role_name}, {org_name}" if org_name else role_name
        position = h.make_position(
            context,
            name=position_name,
            country="cz",
            lang="ces",
        )
        entity.add("position", position_name)

        categorisation = categorise(context, position, is_pep=is_pep)
        if not categorisation.is_pep:
            continue

        # Pick the latest available
        end_date = max(
            filter(None, [wp.get("end"), wp.get("writtenDateOfEnd")]), default=None
        )

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
