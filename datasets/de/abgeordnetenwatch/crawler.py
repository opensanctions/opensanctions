#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import math
from zavod import Context
from zavod import helpers as h


def crawl(context: Context):
    # Fetch the source data URL specified in the metadata to a local path:
    api_response = context.fetch_json(context.dataset.data.url)
    # TODO: Investigate why first query api_response["meta"]["result"]["total"] is 57726
    # total_results = api_response["meta"]["result"]["total"]
    total_results = 2733
    num_batches = math.ceil(total_results / 100)

    for bi in range(num_batches):
        context.log.info(
            "Get Politicians from range {} to {}".format(bi * 100, (bi + 1) * 100)
        )
        if bi > 0:
            api_response = context.fetch_json(
                context.dataset.data.url, params={"range_start": bi * 100}
            )

        for candidate in api_response.get("data"):
            parliament_info = candidate["parliament_period"]["label"].split(" ")
            position = h.make_position(
                context,
                name="parliament_period",
                country="Germany",
                subnational_area=parliament_info[0],
                inception_date=parliament_info[1],
                dissolution_date=parliament_info[3],
            )

            person = context.make("Person")
            person.id = context.make_id(candidate["politician"]["id"])
            h.apply_name(person, full=candidate["politician"]["label"])
            h.make_occupancy(context, person, position, False)
            context.emit(person)
            context.emit(position)
