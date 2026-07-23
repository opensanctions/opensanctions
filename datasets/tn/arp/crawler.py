import json
import re

from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# The hemicycle endpoint is a JSON-RPC "call" that returns an HTML fragment of the seating
# plan. mandat_id 105 is the current mandate under the 2022 constitution.
RPC_BODY = {"jsonrpc": "2.0", "method": "call", "params": {"mandat_id": 105}}

DEPUTY_ID_RE = re.compile(r"deputyid(\d+)")


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Assembly of the Representatives of the People of Tunisia",
        country="tn",
        wikidata_id="Q29169698",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    response = context.fetch_json(
        context.data_url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(RPC_BODY),
        cache_days=1,
    )
    fragment = html.fromstring(response["result"])

    seen: set[str] = set()
    for div in h.xpath_elements(
        fragment, '//div[contains(@class, "hemicycle-deputy")]'
    ):
        div_id = div.get("id") or ""
        match = DEPUTY_ID_RE.search(div_id)
        if match is None:
            raise ValueError(f"Unexpected deputy element id: {div_id!r}")
        deputy_id = match.group(1)
        name = h.element_text(div)
        assert name, f"Empty name for deputy {deputy_id}"
        if deputy_id in seen:
            continue
        seen.add(deputy_id)

        person = context.make("Person")
        person.id = context.make_slug(deputy_id)
        person.add("name", name, lang="ara")
        # Candidacy for the Assembly is a right of every voter born to a Tunisian father
        # or mother (2022 Constitution, Article 58).
        # https://www.constituteproject.org/constitution/Tunisia_2022
        person.add("citizenship", "tn")

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)

    if not seen:
        raise ValueError("No deputies found in the ARP hemicycle response")
