from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import OccupancyStatus, categorise


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    deputies = doc.findall(".//ul[@class='deputies']/li")
    for deputy in deputies:
        link = deputy.find(".//a")
        if link is None:
            continue
        href = link.get("href")
        assert href is not None, "Missing href"
        url = urljoin(context.data_url, href)
        pep_doc = context.fetch_html(url, cache_days=1)
        name = pep_doc.findtext(".//div[@id='title_content']/h1")
        if not name:
            context.log.warning("Missing name for: {}".format(url))
            continue
        entity = context.make("Person")
        entity.id = context.make_id(name, href)
        entity.add("citizenship", "pl")
        entity.add("topics", "role.pep")
        entity.add("sourceUrl", url)
        entity.add("name", name)

        position = h.make_position(
            context,
            name="Member of the Sejm of Poland",
            wikidata_id="Q19269361",
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
