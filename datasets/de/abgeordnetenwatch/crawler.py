#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import math
from nomenklatura.util import is_qid

from zavod import Context
from zavod import helpers as h


def crawl_parliament_label(context: Context, parliament_api_url: str):
    api_response = context.fetch_json(parliament_api_url, cache_days=30)
    parliament = api_response.pop("data")

    return parliament.pop("label_external_long")


def crawl_parliament_period(context: Context, parliament_period_api_url: str):
    api_response = context.fetch_json(parliament_period_api_url, cache_days=30)
    parliament_period = api_response.pop("data")

    parliament = parliament_period.pop("parliament")
    parliament_api_url = parliament.pop("api_url")

    parliament_label = crawl_parliament_label(context, parliament_api_url)

    parliament_data = {}
    parliament_data["label"] = parliament_label
    parliament_data["start_date_period"] = parliament_period.pop("start_date_period")
    parliament_data["end_date_period"] = parliament_period.pop("end_date_period")

    return parliament_data


def crawl_politician(context: Context, politician_api_url: str):
    api_response = context.fetch_json(politician_api_url, cache_days=30)
    politician = api_response.pop("data")
    return politician


def crawl(context: Context):
    # Fetch the source data URL specified in the metadata to a local path:
    api_response = context.fetch_json(context.dataset.data.url, cache_days=30)
    total_results = api_response.pop("meta").pop("result").pop("total")
    num_batches = math.ceil(total_results / 100)

    bi = 0
    while bi < num_batches:
        context.log.info(
            "Get Politicians from range {} to {}".format(bi * 100, (bi + 1) * 100)
        )

        if bi > 0:
            api_response = context.fetch_json(
                context.dataset.data.url,
                params={"range_start": bi * 100},
                cache_days=30,
            )
            # We need to always recheck total number of results because the API
            # returns a incorrect value in the first request without range_start
            batch_total_results = api_response.pop("meta").pop("result").pop("total")
            if batch_total_results != total_results:
                num_batches = math.ceil(batch_total_results / 100)

        for mandate in api_response.get("data"):
            politician = mandate.pop("politician")

            # Get state from parliament info
            parliament_info = mandate.pop("parliament_period")

            # Don't get Members of EU-Parlament
            if "EU" in parliament_info.pop("label"):
                continue

            # Get the Parliament infor in its own endpoint
            parliament_period_data = crawl_parliament_period(
                context, parliament_info.pop("api_url")
            )

            # Create position
            position = h.make_position(
                context,
                name="Member of the {}".format(parliament_period_data["label"]),
                country="Germany",
                subnational_area=state if state != "Bundestag" else None,
            )

            politician_fullname = politician.pop("label")
            context.log.info("Get Politician {} detail".format(politician_fullname))
            politician_detail = crawl_politician(context, politician.pop("api_url"))

            person = context.make("Person")

            politician_wikidata_id = politician_detail.pop("qid_wikidata")
            person.add("wikidataId", politician_wikidata_id)

            if is_qid(politician_wikidata_id):
                person.id = politician_wikidata_id
            else:
                person.id = context.make_id(politician.pop("id"))

            person.add("birthDate", politician_detail.pop("year_of_birth"))
            h.apply_name(person, full=politician_fullname)

            mandate_start_date = mandate.pop(
                "start_date"
            ) or parliament_period_data.pop("start_date_period")
            mandate_end_date = mandate.pop("end_date") or parliament_period_data.pop(
                "end_date_period"
            )

            occupancy = h.make_occupancy(
                context,
                person,
                position,
                True,
                start_date=mandate_start_date,
                end_date=mandate_end_date,
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
                    "abgeordnetenwatch_url",
                    "first_name",
                    "last_name",
                    "birth_name",
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
