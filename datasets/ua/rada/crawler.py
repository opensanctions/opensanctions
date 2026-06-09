from typing import Any
import re

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# get path to all convocations from metadata url
SKL_RE = re.compile(r"^/ogd/mps/skl(\d+)/$")
TOPICS = ["gov.national", "gov.legislative"]


def crawl_member(
    context: Context,
    row: dict[str, Any],
) -> None:
    start_date = row.pop("date_begin")
    if start_date < h.earliest_term_start(TOPICS):
        context.log.info(
            f"Skipping row with start date {start_date} outside coverage window"
        )
        return

    full_name = row.pop("full_name")
    person = context.make("Person")
    person.id = context.make_id(full_name, row.pop("id"))
    h.apply_name(
        person,
        full=full_name,
        first_name=row.pop("first_name"),
        last_name=row.pop("last_name"),
        patronymic=row.pop("second_name"),
        lang="ukr",
    )
    other_name = row.pop("other_name", None)
    if other_name is not None:
        person.add("alias", other_name)

    h.apply_date(person, "birthDate", row.pop("birthday"))
    person.add("gender", row.pop("gender"))
    person.add("political", row.pop("party_name", None))
    person.add("political", row.pop("party_text", None))
    person.add("education", row.pop("education"))
    person.add("sourceUrl", row.pop("anketa_url"))
    person.add("biography", row.pop("anketa_data"))
    # citizenship required: https://zakon.rada.gov.ua/laws/show/254%D0%BA/96-%D0%B2%D1%80#Text
    person.add("citizenship", "ua")

    position = h.make_position(
        context,
        name="Member of the Verkhovna Rada of Ukraine",
        country="ua",
        topics=TOPICS,
        wikidata_id="Q12132454",
    )
    position.add("subnationalArea", row.pop("region_name", None))
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=row.pop("date_end", None),
        no_end_implies_current=True,
    )
    if occupancy is not None:
        occupancy.add("constituency", row.pop("district_text"))
        context.emit(position)
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    metadata = context.fetch_json(context.data_url)
    for item in metadata["item"]:
        match = SKL_RE.match(item.get("path", ""))
        if match is None:
            continue
        convocation = int(match.group(1))
        print(f"Crawling convocation {convocation}...")
        data_url = f"https://data.rada.gov.ua/ogd/mps/skl{convocation}/mps{convocation:02d}-data.json"
        for row in context.fetch_json(data_url):
            crawl_member(context, row)
