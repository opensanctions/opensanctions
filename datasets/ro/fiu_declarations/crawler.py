from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    table = doc.find(".//table[@class='table table-bordered declarations']")
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
