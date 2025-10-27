from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.shed import zyte_api
from zavod.stateful.positions import OccupancyStatus, categorise


def crawl(context: Context) -> None:
    senator_container = ".//div[@class='senator-kontener']"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=senator_container,
        cache_days=1,
    )
    for row in doc.findall(senator_container):
        link = row.find(".//a")
        if link is None:
            continue
        name = link.text.strip()
        href = link.get("href")
        assert href is not None, "Missing href"
        assert name, "Missing name for: {}".format(href)
        url = urljoin(context.data_url, href)
        entity = context.make("Person")
        entity.id = context.make_id(name, href)
        entity.add("citizenship", "pl")
        entity.add("topics", "role.pep")
        entity.add("sourceUrl", url)
        entity.add("name", name)

        position = h.make_position(
            context,
            name="Member of the Senate of Poland",
            wikidata_id="Q81747225",
            country="pl",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            no_end_implies_current=False,
            categorisation=categorisation,
            status=OccupancyStatus.UNKNOWN,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(entity)
