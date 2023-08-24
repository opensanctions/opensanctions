#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import math
from zavod import Context
from zavod import helpers as h


def crawl(context: Context):
    # Fetch the source data URL specified in the metadata to a local path:
    api_response = context.fetch_json(context.dataset.data.url)
    total_results = api_response["meta"]["result"]["total"]
    num_batches = math.ceil(total_results / 100)

    bi = 0
    while bi < num_batches:
        context.log.info(
            "Get Politicians from range {} to {}".format(bi * 100, (bi + 1) * 100)
        )
        if bi > 0:
            api_response = context.fetch_json(
                context.dataset.data.url, params={"range_start": bi * 100}
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
                name="Member of Parliament ({} - {}) - Germany/{}".format(
                    inception_date, dissolution_date, state
                ),
                country="Germany",
                subnational_area=state,
                inception_date=inception_date,
                dissolution_date=dissolution_date,
            )

            person = context.make("Person")
            person.id = context.make_id(candidate["politician"]["id"])
            h.apply_name(person, full=candidate["politician"]["label"])
            h.make_occupancy(context, person, position, False)
            context.emit(person)
            context.emit(position)

        # Increment batch number
        bi += 1
