#!/usr/bin/env python
# -*- coding: utf-8 -*-


from zavod import Context
from zavod import helpers as h


def crawl(context: Context):
    request_response = context.fetch_html(context.dataset.data.url, cache_days=30)

    # Get current judges
    current_judges_block = request_response.xpath(
        "//div[contains(@class, 'accordian')]"
    )[0]
    current_judges = current_judges_block.xpath(
        ".//div[contains(@class, 'fusion-panel')]"
    )

    # First get the list of different types of courts
    courts = []
    for sm in request_response.xpath('.//ul[contains(@class,"sub-menu")]')[:2]:
        for court in [s.text_content() for s in sm.xpath("./li/a/span")]:
            courts.append(court)

    def filter_courts(t):
        """
        Giving a text block, filter the list of courts mentioned in the text
        or remove the excluded courts
        """
        if "all Divisions" in t:
            if "except the" not in t:
                return courts
            else:
                not_in = t.split("except the", maxsplit=1)[1].strip()
                return [c for c in courts if c not in not_in]
        elif "to the" in t:
            division = t.split("to the", maxsplit=1)[1].strip().replace(")", "")
            return [division]

    judges = []
    for judge in current_judges:
        new_judge = {}
        new_judge["name"] = (
            judge.xpath('.//span[contains(@class, "fusion-toggle-heading")]')[0]
            .text.replace("Hon. ", "")
            .split(",")[0]
            .replace(" CBE", "")
        )
        for line in (
            judge.xpath('.//div[contains(@class, "panel-body")]')[0]
            .text_content()
            .splitlines()
        ):
            if "Assigned" in line:
                new_judge["positions"] = filter_courts(line)
                break
            else:
                new_judge["positions"] = []
                for c in courts:
                    if c in line:
                        new_judge["positions"] += [c]

        judges += [new_judge]

        judge_proxy = context.make("Person")
        h.apply_name(judge_proxy, full=new_judge["name"])
        # TODO: Make a better ID
        judge_proxy.id = "judicial-admin-cayman-islands-{}".format(
            new_judge["name"].replace(" ", "_")
        )

        judge_positions = []
        for position in new_judge["positions"]:
            pos = h.make_position(
                context,
                name=position,
                country="Cayman Islands",
            )
            occupancy = h.make_occupancy(
                context,
                judge_proxy,
                pos,
                True,
                start_date="2021",
                end_date="2025",
            )
            judge_positions += [(pos, occupancy)]

        context.emit(judge_proxy, target=True)
        for pos in judge_positions:
            context.emit(pos[0])
            context.emit(pos[1])
