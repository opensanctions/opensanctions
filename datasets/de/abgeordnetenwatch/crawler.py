import math
from rigour.ids.wikidata import is_qid

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


def crawl_parliament(context: Context, parliament_api_url: str):
    api_response = context.fetch_json(parliament_api_url, cache_days=1)
    return api_response.pop("data")


def crawl_parliament_period(context: Context, parliament_period_api_url: str):
    api_response = context.fetch_json(parliament_period_api_url, cache_days=1)
    return api_response.pop("data")


def crawl_politician(context: Context, politician_api_url: str):
    api_response = context.fetch_json(politician_api_url, cache_days=2)
    politician = api_response.pop("data")
    return politician


def crawl(context: Context):
    # Fetch the source data URL specified in the metadata to a local path:
    api_response = context.fetch_json(context.data_url, cache_days=1)
    total_results = api_response.pop("meta").pop("result").pop("total")
    num_batches = math.ceil(total_results / 100)

    bi = 0
    while bi < num_batches:
        context.log.info(
            "Get Politicians from range {} to {}".format(bi * 100, (bi + 1) * 100)
        )

        if bi > 0:
            api_response = context.fetch_json(
                context.data_url,
                params={"range_start": bi * 100},
                cache_days=1,
            )
            # We need to always recheck total number of results because the API
            # returns a incorrect value in the first request without range_start
            batch_total_results = api_response.pop("meta").pop("result").pop("total")
            if batch_total_results != total_results:
                num_batches = math.ceil(batch_total_results / 100)

        for mandate in api_response.get("data"):
            politician = mandate.pop("politician")
            parliament_period = mandate.pop("parliament_period")
            parliament_period_detail = crawl_parliament_period(
                context, parliament_period["api_url"]
            )
            parliament_detail = crawl_parliament(
                context, parliament_period_detail["parliament"]["api_url"]
            )
            parliament_label = parliament_period_detail["parliament"]["label"]
            parliament_label_long = parliament_detail["label_external_long"]

            # Don't get Members of EU-Parlament
            if "EU" in parliament_label:
                continue

            # Create position
            position = h.make_position(
                context,
                name="Member of the {}".format(parliament_label_long),
                country="Germany",
                subnational_area=(
                    parliament_label if parliament_label != "Bundestag" else None
                ),
            )

            politician_fullname = politician.pop("label")
            context.log.info("Get Politician {} detail".format(politician_fullname))
            politician_detail = crawl_politician(context, politician.pop("api_url"))

            person = context.make("Person")

            politician_wikidata_id = politician_detail.pop("qid_wikidata")
            if politician_wikidata_id is not None and is_qid(politician_wikidata_id):
                person.id = politician_wikidata_id
            else:
                person.id = context.make_id(politician.pop("id"))

            person.add("wikidataId", politician_wikidata_id)
            person.add("birthDate", politician_detail.pop("year_of_birth"))
            h.apply_name(
                person,
                full=politician_fullname,
                first_name=politician_detail.pop("first_name"),
                last_name=politician_detail.pop("last_name"),
                maiden_name=politician_detail.pop("birth_name"),
            )

            person.add("sourceUrl", politician_detail.pop("abgeordnetenwatch_url"))

            mandate_start_date = mandate.pop(
                "start_date"
            ) or parliament_period_detail.pop("start_date_period")
            mandate_end_date = mandate.pop("end_date") or parliament_period_detail.pop(
                "end_date_period"
            )
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
                context.emit(person, target=True)
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

        # Increment batch number
        bi += 1
