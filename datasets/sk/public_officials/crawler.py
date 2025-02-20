import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise, OccupancyStatus
from zavod.shed.trans import (
    apply_translit_full_name,
    make_position_translation_prompt,
)

TRANSLIT_OUTPUT = {
    "eng": ("Latin", "English"),
}
POSITION_PROMPT = prompt = make_position_translation_prompt("slk")


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
        context.log.info(f"Table not found for {name_raw}")
        return

    data = {}

    for row in table.findall(".//tr"):
        label_cell = row.find(".//td[@class='label']")
        if label_cell is not None:
            label = label_cell.text_content().strip().strip(":")
            continue  # Move to the next row for the corresponding value

        assert label is not None
        value_cell = row.find(".//td[@class='value']")
        if value_cell is not None:
            # Check if the cell contains <div> elements for positions with multiple values
            divs = value_cell.findall(".//div")
            if divs:
                # Extract position from each <div>
                values = []
                for div in divs:
                    text = div.text_content().strip()
                    values.append(text)
            else:
                # Extract text directly from the cell for single values
                values = value_cell.text_content().strip()
            data[label] = values
        label = None  # Reset the label for the next row
    crawl_person(context, data, href, name_raw)


def crawl_person(context: Context, data, href, name_raw):
    year = data.pop("oznámenie za rok")
    position_slk = data.pop("vykonávaná verejná funkcia")
    int_id = data.pop("Interné číslo")

    person = context.make("Person")
    person.id = context.make_id(name_raw, int_id)
    last_name, name = name_raw.split(",", 1)
    h.apply_name(person, first_name=name.strip(), last_name=last_name.strip())
    person.add("topics", "role.pep")
    person.add("sourceUrl", href)
    for pos in position_slk:
        if "verejný funkcionár, ktorý nie je uvedený v písmenách a) až zo)" in pos:
            pos_looked_up = context.lookup_value("position", pos)
            if pos_looked_up is None:
                context.log.warning(f"Position match not found for {pos}")
            pos = pos_looked_up
        position = h.make_position(
            context,
            pos,
            lang="slk",
            country="SK",
        )
        person.add("position", pos)

        apply_translit_full_name(
            context, position, "slk", pos, TRANSLIT_OUTPUT, POSITION_PROMPT
        )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            return

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=False,
            categorisation=categorisation,
            status=OccupancyStatus.UNKNOWN,
        )
        occupancy.add("date", year)

        context.emit(position)
        context.emit(occupancy)
    context.emit(person)


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
