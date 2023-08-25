#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import math
from zavod import Context
from zavod import helpers as h


def get_politician_data(context: Context, politician_api_url: str):
    api_response = context.fetch_json(politician_api_url, cache_days=30)
    politician = api_response.pop("data")
    context.audit_data(politician)
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
            context.audit_data(mandate)
            politician = mandate.pop("politician")

            # Get state from parliament info
            parliament_info = mandate.pop("parliament_period").pop("label").split(" ")
            state = parliament_info[0]

            # Create position
            position = h.make_position(
                context,
                name="Member of Parliament",
                country="Germany",
                subnational_area=state,
            )

            person = context.make("Person")
            person.id = context.make_id(politician.pop("id"))
            politician_fullname = politician.pop("label")

            # Get the politician birthday date
            context.log.info("Get Politician {} birth year".format(politician_fullname))
            politician_data = get_politician_data(context, politician.pop("api_url"))
            birthYear = politician_data.pop("year_of_birth")
            person.add("birthDate", birthYear)

            # Apply politician fullname to Person entity
            h.apply_name(person, full=politician_fullname)

            # Create occupancy
            occupancy = h.make_occupancy(
                context, person, position, False, start_date=mandate.pop("start_date")
            )
            if occupancy:
                context.emit(person, target=True)
                context.emit(position)
                context.emit(occupancy)

        # Increment batch number
        bi += 1
