from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h


def crawl_row(context: Context, position: Entity, row, url: str) -> None:
    str_row = h.cells_to_str(row)
    name = str_row.pop("name")
    in_house_group = str_row.pop("in_house_group")
    area = str_row.pop("constituency_area")
    entity = context.make("Person")
    entity.id = context.make_id(name, in_house_group, area)
    entity.add("name", name)
    entity.add("sourceUrl", url)
    entity.add("topics", "role.pep")
    entity.add("citizenship", "jp")
    entity.add("political", in_house_group)

    occupancy = h.make_occupancy(context, entity, position, no_end_implies_current=True)
    if occupancy is not None:
        occupancy.add("constituency", area)
        context.emit(occupancy)

    context.emit(entity)
    context.audit_data(str_row)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of Japan",
        wikidata_id="Q17506823",
        country="jp",
        topics=["gov.legislative", "gov.national"],
    )
    context.emit(position)

    doc = context.fetch_html(context.data_url)
    urls = [context.data_url]
    nav_links = h.xpath_elements(
        doc, './/div[@id="LnaviArea"]//table//a', expect_exactly=17
    )
    assert nav_links, nav_links
    for a in nav_links:
        href = a.get("href")
        if href is None:
            continue
        urls.append(urljoin(context.data_url, href))

    for url in urls:
        doc = context.fetch_html(url)
        table = h.xpath_element(doc, './/div[@id="MainContentsArea"]/table')
        for row in h.parse_html_table(table):
            crawl_row(context, position, row, url)
