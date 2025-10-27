from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Canada",
        wikidata_id="Q18524027",
        country="ca",
        topics=["gov.legislative", "gov.national"],
    )
    context.emit(position)
    doc = context.fetch_html(context.data_url)
    for row in doc.findall(".//table//tr"):
        link = row.find(".//a")
        if link is None:
            continue
        cells = row.findall(".//td")
        cells = [h.element_text(c) for c in cells]
        href = link.get("href")
        assert href is not None, "Missing href"
        url = urljoin(context.data_url, href)
        entity = context.make("Person")
        entity.id = context.make_id(href, cells[2])
        entity.add("citizenship", "ca")
        entity.add("topics", "role.pep")
        entity.add("sourceUrl", url)

        entity.add("name", cells[0])
        entity.add("political", cells[1])
        entity.add("address", cells[2])

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=cells[3],
            end_date=cells[4],
            no_end_implies_current=True,
            propagate_country=False,
        )
        if occupancy:
            context.emit(occupancy)
            context.emit(entity)
