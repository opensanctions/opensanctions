from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.extract import zyte_api


def crawl(context: Context):
    table_xpath = ".//table[@class='table table-bordered declarations']"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        table_xpath,
        cache_days=1,
        absolute_links=True,
        geolocation="RO",
    )
    table = doc.find(table_xpath)
    rows = table.xpath("./tr")
    for row in rows:
        first_td = row.xpath("./td[1]")
        if first_td:
            name = first_td[0].text_content().strip()

            pep = context.make("Person")
            pep.id = context.make_id(name)
            pep.add("name", name)
            pep.add("country", "ro")
            pep.add("topics", "role.pep")
            position = h.make_position(
                context,
                name="Financial Intelligence Unit Official",
                country="ro",
            )
            categorisation = categorise(context, position, True)
            if categorisation:
                occupancy = h.make_occupancy(
                    context,
                    pep,
                    position,
                    categorisation=categorisation,
                )

            context.emit(position)
            context.emit(occupancy)
            context.emit(pep)
