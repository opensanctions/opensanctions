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
        judge_proxy.add("sourceUrl", context.dataset.data.url)
        h.apply_name(judge_proxy, full=new_judge["name"])
        judge_proxy.id = context.make_id(new_judge["name"])

        judge_positions = []
        for position in new_judge["positions"]:
            pos = h.make_position(
                context,
                name="Judge for {}".format(position),
                country="Cayman Islands",
            )
            occupancy = h.make_occupancy(context, judge_proxy, pos, True)
            judge_positions += [(pos, occupancy)]

        # If there is not information about the judge position
        # we just add tje position 'Judge'
        if len(judge_positions) == 0:
            pos = h.make_position(
                context,
                name="Judge",
                country="Cayman Islands",
            )
            occupancy = h.make_occupancy(context, judge_proxy, pos, True)
            judge_positions += [(pos, occupancy)]

        context.emit(judge_proxy, target=True)
        for pos in judge_positions:
            context.emit(pos[0])
            context.emit(pos[1])

    # Get Chief Justice
    chief_justice_url = "https://www.judicial.ky/judicial-administration/chief-justice"
    request_response = context.fetch_html(chief_justice_url, cache_days=30)
    chief_justice_name = (
        request_response.xpath(
            './/div[contains(@class, "fusion-flex-column-wrapper-legacy")]'
        )[2]
        .text_content()
        .split(",")[0]
        .replace("Hon. ", "")
    )

    judge_proxy = context.make("Person")
    judge_proxy.add("sourceUrl", chief_justice_url)
    h.apply_name(judge_proxy, full=chief_justice_name)
    judge_proxy.id = context.make_id(chief_justice_name)

    pos = h.make_position(
        context,
        name="Chief Justice",
        country="Cayman Islands",
    )
    occupancy = h.make_occupancy(context, judge_proxy, pos, True)

    context.emit(judge_proxy, target=True)
    context.emit(pos)
    context.emit(occupancy)

    # Get President - Court of Appeal
    president_court_url = (
        "https://www.judicial.ky/judicial-administration/president-court-of-appeal"
    )
    request_response = context.fetch_html(president_court_url, cache_days=30)
    president_court_name = (
        request_response.xpath('.//h4[contains(@class, "panel-title")]')[0]
        .text_content()
        .split("Sir")[-1]
        .strip()
    )

    judge_proxy = context.make("Person")
    judge_proxy.add("sourceUrl", president_court_url)
    h.apply_name(judge_proxy, full=president_court_name)
    judge_proxy.id = context.make_id(president_court_name)

    pos = h.make_position(
        context,
        name="President of the Court of Appeal",
        country="Cayman Islands",
    )
    occupancy = h.make_occupancy(context, judge_proxy, pos, True)
    context.emit(judge_proxy, target=True)
    context.emit(pos)
    context.emit(occupancy)

    # Get Justices of Appeal
    justices_of_appeal_url = (
        "https://www.judicial.ky/judicial-administration/justices-of-appeal"
    )
    request_response = context.fetch_html(justices_of_appeal_url, cache_days=30)
    current_justices_block = request_response.xpath(
        './/div[contains(@class, "accordian")]'
    )[0]

    for justice in current_justices_block.xpath("//h4"):
        judge_proxy = context.make("Person")
        judge_proxy.add("sourceUrl", justices_of_appeal_url)

        # Get name
        justice_name = ""

        justice_text = justice.text_content()
        if "Sir" in justice_text:
            justice_name = justice_text.split("Sir")[-1].strip()
        else:
            justice_name = justice_text.split("Hon.")[-1].replace("Justice", "").strip()

        h.apply_name(judge_proxy, full=justice_name)
        judge_proxy.id = context.make_id(justice_name)

        pos = h.make_position(
            context,
            name="Justice of Appeal",
            country="Cayman Islands",
        )
        occupancy = h.make_occupancy(context, judge_proxy, pos, True)
        context.emit(judge_proxy, target=True)
        context.emit(pos)
        context.emit(occupancy)
