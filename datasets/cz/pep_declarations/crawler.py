from typing import Dict, Any

from requests.exceptions import HTTPError

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


IGNORE = [
    # Name composed from the parts above.
    "fullName",
    # Declaration-filing records (private, canOpen=false) and metadata about
    # them - not biographical data about the official.
    "statements",
    "hasPoint3",
    "hasRequestStatement",
    "hasSecretStatement",
    # Visibility metadata for the declaration record (e.g. SECRET, REQUEST),
    # mirroring the statement flags above - not biographical data.
    "visibility",
    # Whether the person currently holds any registered position.
    "active",
    # Flattened, human-readable duplicates of workingPositions[].
    "concatenatedWorkingPositions",
    "concatenatedWorkingPositionDates",
    "concatenatedWorkingPositionOrganizations",
    "workingFrom",
    "workingTo",
    # Person-level summary flags; the per-position deputy/senator flags inside
    # workingPositions[].workingPosition are used instead.
    "deputy",
    "senator",
    "deputyAndOthers",
    "senatorAndOthers",
    "judge",
    "government",
]


def crawl_person(context: Context, item: Dict[str, Any]) -> None:
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
    entity.add("citizenship", "cz")
    for wp in item.pop("workingPositions", []):
        wp_data = wp.pop("workingPosition")
        # is_deputy and is_senator are position-level flags
        is_deputy = wp_data.pop("deputy")
        is_senator = wp_data.pop("senator")
        # Only deputies and senators are marked as PEP by default
        if is_deputy or is_senator:
            is_pep = True
        else:
            is_pep = None

        role_name = wp_data.pop("name")
        org_name = wp.pop("organization")
        # Construct a clean position_name:
        # e.g. Poslanec, Kancelář Poslanecké sněmovny Parlamentu
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
        entity.add("position", position_name)

        categorisation = categorise(context, position, default_is_pep=is_pep)
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
        if occupancy is not None:
            context.emit(
                entity,
                origin=f"https://cro.justice.cz/verejnost/api/funkcionari/{person_id}",
            )
            context.emit(position)
            context.emit(occupancy)

    context.audit_data(item, ignore=IGNORE)


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
                crawl_person(context, detail)
            except HTTPError as e:
                if e.response.status_code == 404:
                    context.log.info("Failed to fetch detail", item=item, error=e)
                else:
                    raise

        page += 1
