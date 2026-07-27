from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h


def crawl(context: Context) -> None:
    table_xpath = ".//table[@class='table table-bordered declarations']"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        table_xpath,
        cache_days=1,
        absolute_links=True,
        geolocation="RO",
    )
    table = h.xpath_element(doc, table_xpath)
    # Rows are direct children of the table in the source HTML, but Zyte's
    # browser rendering normalises the DOM and inserts a <tbody>, moving the
    # rows under it. Match both layouts so the crawler works either way.
    rows = h.xpath_elements(table, "./tr | ./tbody/tr")
    for row in rows:
        first_td = h.xpath_elements(row, "./td[1]")
        if first_td:
            name = h.element_text(first_td[0])

            pep = context.make("Person")
            pep.id = context.make_id(name)
            pep.add("name", name)
            pep.add("citizenship", "ro")
            pep.add("topics", "role.pep")
            position = h.make_position(
                context,
                name="Financial Intelligence Unit Official",
                country="ro",
            )
            categorisation = categorise(context, position, default_is_pep=True)
            if categorisation:
                occupancy = h.make_occupancy(
                    context,
                    pep,
                    position,
                    categorisation=categorisation,
                )
                if occupancy is not None:
                    context.emit(occupancy)

            context.emit(position)
            context.emit(pep)
