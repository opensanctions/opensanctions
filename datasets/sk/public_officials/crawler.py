import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def parse_details(context: Context, link_el):
    href = link_el.get("href")
    name_raw = link_el.text_content().strip()
    # Filter out non-personal links
    if re.search(r"#section_[A-Za-z]+$", href) or href.endswith("ViewType=2#top"):
        return
    doc = context.fetch_html(href, cache_days=2)
    table = doc.find(".//table[@class='oznamenie_table']")
    if table is None:
        # Some pages are in development
        context.log.warning(f"Table not found for {name_raw}")
        return

    data = {}

    for row in table.findall(".//tr"):
        label_cell = row.find(".//td[@class='label']")
        if label_cell is not None:
            label = label_cell.text_content().strip().strip(":")
            continue  # Move to the next row for the corresponding value

        value_cell = row.find(".//td[@class='value']")
        if value_cell is not None and label:
            # Extract each div's text inside value_cell
            values = []
            for div in value_cell.findall(".//div"):
                text = div.text_content().strip()
                values.append(text)
            data[label] = values
            label = None  # Reset the label for the next row

    crawl_person(context, data, href, name_raw)


def crawl_person(context: Context, data, href, name_raw):
    # name = data.pop("titul, meno, priezvisko") # name with title
    # year = data.pop("oznámenie za rok")
    position = data.pop("vykonávaná verejná funkcia")
    person = context.make("Person")
    person.id = context.make_id(name_raw)
    last_name, name = name_raw.split(",", 1)
    h.apply_name(person, first_name=name.strip(), last_name=last_name.strip())
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
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )

    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)


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
    for link_el in link_elements:
        results_doc.make_links_absolute(context.data_url)
        parse_details(context, link_el)
