#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import math
from zavod import Context
from zavod import helpers as h


def crawl(context: Context):
    # Fetch the source data URL specified in the metadata to a local path:
    api_response = context.fetch_json(context.dataset.data.url, cache_days=30)
    total_results = api_response["meta"]["result"]["total"]
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
            batch_total_results = api_response["meta"]["result"]["total"]
            if batch_total_results != total_results:
                num_batches = math.ceil(batch_total_results / 100)

        for candidate in api_response.get("data"):
            parliament_info = candidate["parliament_period"]["label"].split(" ")
            state = parliament_info[0]
            inception_date = parliament_info[1]
            dissolution_date = parliament_info[3]

            position = h.make_position(
                context,
                name="Member of Parliament",
                country="Germany",
                subnational_area=state,
                inception_date=inception_date,
                dissolution_date=dissolution_date,
            )

            person = context.make("Person")
            person.id = context.make_id(candidate["politician"]["id"])

            # Get the politician birthday date
            context.log.info(
                "Get Politician {} birth year".format(candidate["politician"]["label"])
            )
            api_response = context.fetch_json(
                candidate["politician"]["api_url"], cache_days=30
            )
            birthYear = api_response["data"]["year_of_birth"]
            person.add("birthDate", birthYear)

            # Get fullname
            h.apply_name(person, full=candidate["politician"]["label"])
            occupancy = h.make_occupancy(
                context, person, position, False, start_date=candidate["start_date"]
            )
            if occupancy:
                context.emit(person, target=True)
                context.emit(position)
                context.emit(occupancy)

        # Increment batch number
        bi += 1
