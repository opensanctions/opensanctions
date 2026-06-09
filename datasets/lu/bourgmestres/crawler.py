from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

DATASETS_QUERY_URL = "https://data.public.lu/api/1/datasets/?lang=en&q=bourgmestre"


def crawl_record(
    context: Context,
    record: dict[str, Any],
) -> None:
    commune_code = record.pop("COM_CODE")
    commune_label = record.pop("COM_LABEL")
    position_label = record.pop("MAN_LABEL")
    first_name = record.pop("ELU_FIRST_NAME")
    last_name = record.pop("ELU_LAST_NAME")
    gender = record.pop("ELU_SEX")
    start_date = record.pop("COAC_START_DATE")
    end_date = record.pop("COAC_END_DATE")

    res = context.lookup("position", position_label)
    assert res is not None, f"Unknown position: {position_label!r}"
    position_name = res.value

    position = h.make_position(
        context,
        name=f"{position_name} of {commune_label}",
        country="lu",
        subnational_area=commune_label,
        lang="fra",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, commune_code)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="fra")
    person.add("gender", gender)
    # citizenship not required: https://guichet.public.lu/fr/citoyens/citoyennete/elections/elections-communales/candidat-elections-communales.html
    person.add("country", "lu")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date or None,
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(position)
        context.emit(occupancy)
        context.emit(person)

    context.audit_data(record)


def crawl(context: Context) -> None:
    response = context.fetch_json(DATASETS_QUERY_URL, cache_days=1)
    assert isinstance(response, dict)

    # If there is more than one dataset for our search for "bourgmestre", a new election may have happened.
    total = response["total"]
    if total != 1:
        context.log.warning(
            "Expected exactly one dataset, found %d — a new election may have happened",
            total,
        )
        raise
    dataset = response["data"][0]

    # If the dataset page URL has changed, something changed, we want to bail
    if dataset["page"] != context.data_url:
        context.log.error(
            "Dataset page URL has changed — a new election may have happened: %s",
            dataset["page"],
        )
        raise

    json_resource = next(
        (r for r in dataset["resources"] if r["format"] == "json"),
        None,
    )
    assert json_resource is not None, "No JSON resource found in dataset"

    data = context.fetch_json(json_resource["url"], cache_days=1)
    assert isinstance(data, list), f"Expected list, got {type(data)}"

    for record in data:
        crawl_record(context, record)
