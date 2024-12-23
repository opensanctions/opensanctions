from zavod import Context

# from zavod.logic.pep import categorise


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    table = doc.find(".//table[@class='table table-bordered declarations']")
    rows = table.xpath("./tr")
    for row in rows:
        first_td = row.xpath("./td[1]")  # Get the first <td> under the current <tr>
        if first_td:
            # Extract and clean the text content
            name = first_td[0].text_content().strip()
            # categorisation = categorise(context, position, True)

            pep = context.make("Person")
            pep.id = context.make_id(name)
            pep.add("name", name)
            pep.add("country", "ro")
            pep.add("topics", "role.pep")
            context.emit(pep)
