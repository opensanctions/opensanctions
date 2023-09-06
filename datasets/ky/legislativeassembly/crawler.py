#!/usr/bin/env python
# -*- coding: utf-8 -*-

from zavod import Context
from zavod import helpers as h


def crawl(context: Context):
    request_response = context.fetch_html(context.dataset.data.url, cache_days=30)

    # The list of positions sections that we are interested
    positions = [
        "Speaker of Parliament",
        "Premier",
        "Deputy Premier",
        "Cabinet Ministers",
        "Ex Officio",
    ]

    # The HTML is malformed and it's easier to parse the text content
    # instead of the HTML
    page_text = request_response.text_content()

    # After the following part we are next to the legislative positions
    initial_pad = page_text.find("You are here")
    page_text = page_text[initial_pad:]

    # Find the indexes in the text were each position is written
    positions_idx = []
    for pos in positions:
        positions_idx += [(pos, page_text.find(pos))]

    def get_positions(s):
        """
        Find information about positions from a string
        """
        positions_init = ["Minister", "Member", "Premier", "Deputy"]
        p = []
        new_pos = []

        # Split text in words
        words = s.split(" ")

        advance = False
        for i, w in enumerate(words):
            # We need to advance when the position is Deputy, because
            # the next word is Premier and the position is 'Deputy Premier'
            # and not 'Deputy' and 'Premier'
            if advance:
                advance = False
                new_pos += [w]
                continue
            if w == "Deputy":
                advance = True

            # We get all the subsequent words until we find a new word
            # representing a position
            if i > 0 and w in positions_init or i == len(words) - 1:
                # We found another position, save what we have until now
                # and start getting a new position. Remove ' and' from the
                # position if it exists.
                if i == len(words) - 1:
                    new_pos += [w]
                full_new_pos = " ".join(new_pos).removesuffix(" and").strip()
                p += [full_new_pos]
                new_pos = [w]
            else:
                # Add word to position description
                new_pos += [w]
        return p

    persons = []
    for i in range(len(positions_idx) - 1):
        if positions_idx[i][0] != "Cabinet Ministers":
            parts = (
                page_text[positions_idx[i][1] : positions_idx[i + 1][1]]
                .split("Hon.")[1]
                .split(",")
            )
            new_person = {}
            new_person["name"] = parts[0].strip()
            positions = parts[1:]
            new_person["positions"] = [
                get_positions(p.replace("MP", "").strip())
                for p in positions
                if "MP" in p
            ][0]
            persons += [new_person]
        else:
            c = page_text[positions_idx[i][1] : positions_idx[i + 1][1]]
            for cm in c.split("Hon.")[1:]:
                cm_parts = cm.split("MP,")
                new_person = {}
                new_person["name"] = cm_parts[0].split(",")[0].strip()
                new_person["positions"] = [
                    p.strip() for p in "".join(cm_parts[1:]).strip().split(" and ")
                ]

                persons += [new_person]

    for person in persons:
        person_proxy = context.make("Person")
        h.apply_name(person_proxy, full=person["name"])
        person_proxy.id = context.make_id(person["name"])

        person_positions = []
        for position in person["positions"]:
            pos = h.make_position(
                context,
                name=position if "Member" not in position else "Member of Parliament",
                country="Cayman Islands",
            )
            occupancy = h.make_occupancy(
                context,
                person_proxy,
                pos,
                True,
                start_date="2021",
                end_date="2025",
            )
            person_positions += [(pos, occupancy)]

        context.emit(person_proxy, target=True)
        for pos in person_positions:
            context.emit(pos[0])
            context.emit(pos[1])
