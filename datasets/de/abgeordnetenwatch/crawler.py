#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import math
from zavod import Context
from zavod import helpers as h


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
            parliament_info = mandate.pop("parliament_period").pop("label").split(" ")
            state = parliament_info[0]

            # Create position
            position = h.make_position(
                context,
                name="Member of the {} Parliament".format(state),
                country="Germany",
                subnational_area=state,
            )

            person = context.make("Person")
            person.id = context.make_id(politician.pop("id"))
            politician_fullname = politician.pop("label")

            # Get the politician birthday date
            context.log.info("Get Politician {} birth year".format(politician_fullname))
            politician_data = crawl_politician(context, politician.pop("api_url"))
            birthYear = politician_data.pop("year_of_birth")
            person.add("birthDate", birthYear)

            # Get WikiData ID
            politician_wikidata_id = politician_data.pop("qid_wikidata")
            person.add("wikidataId", politician_wikidata_id)

            # Apply politician fullname to Person entity
            h.apply_name(person, full=politician_fullname)

            # Create occupancy
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                True,
                start_date=mandate.pop("start_date"),
                end_date=mandate.pop("end_date"),
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
                politician_data,
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
