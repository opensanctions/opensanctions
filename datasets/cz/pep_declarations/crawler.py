from typing import Any

from requests.exceptions import HTTPError

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


def crawl_person(context: Context, item: dict[str, Any], source_url: str) -> None:
    person_id = item.pop("id")
    first_name = item.pop("firstName")
    last_name = item.pop("lastName")
    middle_name = item.pop("middleName", None)
    # The source uses placeholder punctuation ("-", ";") in the middle name
    # field to indicate the absence of a middle name; treat these as empty.
    if middle_name is not None and middle_name.strip() in ("-", ";"):
        middle_name = None

    entity = context.make("Person")
    entity.id = context.make_id(person_id)
    h.apply_name(
        entity,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
    )
    entity.add("title", item.pop("titleBefore", None))
    entity.add("nameSuffix", item.pop("titleAfter", None))
    entity.add("sourceUrl", source_url)

    has_occupancy = False
    is_legislator = False
    for wp in item.pop("workingPositions", []):
        wp_data = wp.pop("workingPosition")
        role_name = wp_data.pop("name")
        is_deputy = wp_data.pop("deputy")
        is_senator = wp_data.pop("senator")
        if is_deputy or is_senator:
            res = context.lookup("position", role_name, warn_unmatched=True)
            assert res is not None, role_name
            position_name = res.name
            position = h.make_position(
                context,
                name=res.name,
                country="cz",
                topics=res.topics,
                wikidata_id=res.wikidata_id,
                lang="eng",
            )
            categorisation = categorise(context, position)
        else:
            # The source mixes PEP and non-PEP roles, so these are left to the
            # review UI to categorise.
            org_name = wp.pop("organization")
            # e.g. Ředitel odboru, Ministerstvo zemědělství
            position_name = (
                f"{role_name.capitalize()}, {org_name}" if org_name else role_name
            )
            position = h.make_position(
                context,
                name=position_name,
                country="cz",
                lang="ces",
                translate_name=True,
            )
            categorisation = categorise(context, position, default_is_pep=None)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=wp.pop("start"),
            # writtenDateOfEnd and dateOfEnd: look like the written/formal
            # submission date not the actual end date of the position
            end_date=wp.pop("end", None),
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        has_occupancy = True
        if is_deputy or is_senator:
            is_legislator = True
        if "gov.judicial" in categorisation.topics:
            entity.add("topics", "role.judge")
        entity.add("position", position_name)
        context.emit(position)
        context.emit(occupancy)

    if not has_occupancy:
        return
    if is_legislator:
        # Only Czech citizens can be elected to either chamber: Constitution of
        # the Czech Republic, Art. 19(1) and (2).
        # https://www.psp.cz/docs/laws/constitution.html
        entity.add("citizenship", "cz")
    else:
        entity.add("country", "cz")
    context.emit(entity)


def crawl(context: Context) -> None:
    page = 0
    while True:
        url = f"{context.data_url}?sort=created&order=DESC&page={page}&pageSize=100"
        data = context.fetch_json(url)
        assert data is not None, "Expected JSON response"

        items = data.pop("items")
        if not items:
            break

        for item in items:
            # The list endpoint only returns flattened summary fields; the
            # structured workingPositions array lives on the detail endpoint.
            detail_url = f"{context.data_url}/{item['id']}"
            # As of 2024-06-17, one detail page returns a 404 despite being listed in the
            # declaration list for a few days.
            # https://cro.gov.cz/verejnost/api/funkcionari/5a7e5a80-6a0e-493a-9d79-ca7ffef33778
            # 'concatenatedWorkingPositionOrganizations': 'Obec Laškov'
            # 'concatenatedWorkingPositions': 'starosta'
            # 'concatenatedWorkingPositionDates': '07. 11. 2014 - 31. 10. 2018'
            # There's nothing in the list data indicating when to skip, so skip on 404
            # and rely on min assertion for this not to hide a bug resulting in us
            # skipping everyone.

            try:
                detail = context.fetch_json(detail_url, cache_days=14)
                assert detail is not None, "Expected JSON response"
                crawl_person(context, detail, detail_url)
            except HTTPError as e:
                if e.response.status_code in [404, 500]:
                    context.log.info("Failed to fetch detail", item=item, error=e)
                else:
                    raise

        page += 1
