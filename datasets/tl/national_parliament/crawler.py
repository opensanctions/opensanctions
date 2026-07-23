import re

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

# The site is unreachable from ordinary egress (TCP timeout — geo/host-restricted), so it
# is fetched through the Zyte API with browser rendering.
UNBLOCK_VALIDATOR = './/table[@id="datatable-1"]'

NODE_ID_RE = re.compile(r"/node/(\d+)")


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Parliament of Timor-Leste",
        country="tl",
        wikidata_id="Q19966812",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=UNBLOCK_VALIDATOR,
        cache_days=1,
    )
    table = h.xpath_element(doc, UNBLOCK_VALIDATOR)

    count = 0
    for row in h.xpath_elements(table, ".//tr[td]"):
        cells = h.xpath_elements(row, "./td")
        if len(cells) < 4:
            continue
        name = h.element_text(cells[1])
        if not name:
            continue
        role = h.element_text(cells[2])
        party = h.element_text(cells[3])
        node_ids = h.xpath_strings(cells[1], ".//a/@href")
        match = NODE_ID_RE.search(node_ids[0]) if node_ids else None

        person = context.make("Person")
        if match is not None:
            person.id = context.make_slug(match.group(1))
        else:
            person.id = context.make_id(name, party)
        person.add("name", name, lang="por")
        person.add("political", party or None)
        # Every citizen over seventeen has the right to be elected (Constitution of the
        # RDTL, Section 47(1)). https://www.constituteproject.org/constitution/East_Timor_2002
        person.add("citizenship", "tl")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        if role:
            occupancy.add("description", role, lang="por")
        context.emit(occupancy)
        context.emit(person)
        count += 1

    if count == 0:
        raise ValueError("No deputies parsed from the legislature roster table")
