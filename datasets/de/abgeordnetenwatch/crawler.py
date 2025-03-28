from typing import Any, Dict
from rigour.ids.wikidata import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


def make_position(context: Context, parliamentary_period):
    parliament_detail = context.fetch_json(
        parliamentary_period["parliament"]["api_url"],
        cache_days=1,
    ).pop("data")
    parliament_label = parliamentary_period["parliament"]["label"]
    parliament_label_long = parliament_detail["label_external_long"]

    # Don't get Members of EU-Parlament
    if "EU" in parliament_label:
        return

    # Create position
    subnational_area = parliament_label if parliament_label != "Bundestag" else None
    return h.make_position(
        context,
        name=f"Member of the {parliament_label_long}",
        country="Germany",
        subnational_area=subnational_area,
    )


def crawl_mandate(context: Context, mandate: Dict[str, Any]):
    politician = mandate.pop("politician")
    period = mandate.pop("parliament_period")
    period_detail = context.fetch_json(period["api_url"], cache_days=1).pop("data")
    position = make_position(context, period_detail)
    if not position:
        return

    politician_url = politician.pop("api_url")
    context.log.info(f"Getting politician {politician_url}")
    politician_detail = context.fetch_json(politician_url, cache_days=1).pop("data")

    # Person
    person = context.make("Person")
    wikidata_id = politician_detail.pop("qid_wikidata")
    if wikidata_id is not None and is_qid(wikidata_id):
        person.id = wikidata_id
    else:
        person.id = context.make_id(politician.pop("id"))

    person.add("wikidataId", wikidata_id)
    person.add("birthDate", politician_detail.pop("year_of_birth"))
    h.apply_name(
        person,
        full=politician_detail.pop("label"),
        first_name=politician_detail.pop("first_name"),
        last_name=politician_detail.pop("last_name"),
        maiden_name=politician_detail.pop("birth_name"),
    )
    person.add("sourceUrl", politician_detail.pop("abgeordnetenwatch_url"))

    # Occupancy
    mandate_start_date = mandate.pop("start_date") or period_detail["start_date_period"]
    mandate_end_date = mandate.pop("end_date") or period_detail["end_date_period"]
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        True,
        start_date=mandate_start_date,
        end_date=mandate_end_date,
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)

    # Audit mandate
    context.audit_data(
        mandate,
        ignore=[
            "id",
            "entity_type",
            "api_url",
            "label",
            "id_external_administration",
            "id_external_administration_description",
            "type",
            "info",
            "electoral_data",
            "fraction_membership",
        ],
    )

    # Audit
    context.audit_data(
        politician_detail,
        ignore=[
            "id",
            "entity_type",
            "label",
            "api_url",
            "sex",
            "party",
            "party_past",
            "education",
            "residence",
            "occupation",
            "statistic_questions",
            "statistic_questions_answered",
            "ext_id_bundestagsverwaltung",
            "field_title",
        ],
    )


def crawl(context: Context):
    # Fetch the source data URL specified in the metadata to a local path:
    total_mandates = None
    range_start = 0
    page_size = 100

    while total_mandates is None or range_start < total_mandates:
        context.log.info(f"Get {page_size} mandates from {range_start}")
        req_data = {
            "range_start": range_start,
            "range_end": page_size,  # range_end really seems to mean "page size"
        }
        api_response = context.fetch_json(context.data_url, params=req_data)
        total_mandates = api_response["meta"]["result"]["total"]
        for mandate in api_response.get("data"):
            crawl_mandate(context, mandate)
        range_start += page_size
