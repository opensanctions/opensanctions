import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def crawl(context: Context):

    doc = context.fetch_html(context.data_url, cache_days=2)
    # Get necessary form fields
    viewstate = doc.xpath('//input[@name="__VIEWSTATE"]/@value')[0]
    eventvalidation = doc.xpath('//input[@name="__EVENTVALIDATION"]/@value')[0]

    # Prepare form data for POST request
    form_data = {
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
    }

    results_doc = context.fetch_html(
        context.data_url,
        method="POST",
        data=form_data,
        cache_days=2,
    )
    link_elements = results_doc.xpath(
        '//div[@id="_sectionLayoutContainer__panelContent"]//a[@href]'
    )
    for link in link_elements:
        results_doc.make_links_absolute(context.data_url)
        href = link.get("href")
        name_raw = link.text_content().strip()
        # Filter out non-personal links
        if re.search(r"#section_[A-Za-z]+$", href) or href.endswith("ViewType=2#top"):
            # We could also split positions on https://www.nrsr.sk/web/Default.aspx?sid=vnf%2fzoznam&ViewType=2#top
            continue
        doc = context.fetch_html(href, cache_days=2)
        table = doc.find(".//table[@class='oznamenie_table']")
        if table is None:
            # Some pages are in development
            context.log.warning(f"Table not found for {name_raw}")
            continue

        data = {}

        # Iterate over the rows in the table
        for row in table.findall(".//tr"):
            # Check for label
            label_cell = row.find(".//td[@class='label']")
            if label_cell is not None:
                # Extract and clean the label text
                label = label_cell.text_content().strip().strip(":")
                continue  # Move to the next row for the corresponding value

            # Check for value
            value_cell = row.find(".//td[@class='value']")
            if value_cell is not None and label:
                # Extract and clean the value text
                value = value_cell.text_content().strip()
                data[label] = value
                label = None  # Reset the label for the next row

        # name = data.pop("titul, meno, priezvisko") # name with title
        # year = data.pop("oznámenie za rok")
        position = data.pop("vykonávaná verejná funkcia")
        person = context.make("Person")
        person.id = context.make_id(name_raw)
        person.add("name", name_raw)
        person.add("position", position)
        person.add("topics", "role.pep")
        person.add("sourceUrl", href)

        position = h.make_position(
            context,
            position,
            lang="slk",
            country="SK",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
        )

        context.emit(person)
        context.emit(position)
        context.emit(occupancy)
