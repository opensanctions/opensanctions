from zavod import Context, helpers as h


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    table = doc.find(".//table[@class='table table-bordered declarations']")
    for row in table.findall(".//tr"):
        tds = row.findall("./td")
        if not tds:
            continue
        name = tds[0].text_content().strip()
        # Filter out non-names (e.g., "Declaratie de...")
        if name and not name.startswith("Declaratie de"):
            pep = context.make("Person")
            pep.id = context.make_id(name)
            pep.add("name", name)
            pep.add("country", "ro")
            pep.add("topics", "role.pep")
            context.emit(pep)
