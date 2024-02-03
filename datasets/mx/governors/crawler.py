import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

def crawl_item(input_html, context: Context):
    """
    Creates an entity, a position and a occupancy from the raw HTMLElement.

    :param input_html: The div representing that governor
    :param context: The context object.
    """

    # The easiest way to get the data from the HTML is surprisingly from the text of the class
    # All the information in the web site is written there in the form:
    # col-xs-12 col-sm-6 col-md-4 pb15 mix {name of the governor} [{state}] {start_date} {end_date}
    # We will then use a regex to extract the name, state, start_date and end_date

    regex_pattern = r'col-xs-12 col-sm-6 col-md-4 pb15 mix (.*?) \[(.*?)\] (\d{2}/\d{2}/\d{4}) (\d{2}/\d{2}/\d{4})'

    match = re.match(regex_pattern, input_html.get('class'))

    if match is None:
        context.log.warning("Unable to extract information from HTML element")

    name, state, start_date, end_date = match.group(1), match.group(2), match.group(3), match.group(4)

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)

    name_of_position = "Governor of " + state.title()
    position = h.make_position(context, name_of_position, country="mx")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=True,
            start_date=h.parse_date(start_date, formats=["%d/%m/%Y"])[0],
            end_date=h.parse_date(end_date, formats=["%d/%m/%Y"])[0],
            categorisation=categorisation,
        )

    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)
    

def crawl(context: Context):
    """
    Entrypoint to the crawler.

    The crawler works by first downloading the HTML of the page
    and then iterating for each div that represents a Mexico State Governor,
    to create the entities and positions.

    :param context: The context object.
    """

    response = context.fetch_html(context.data_url)

    if response is None:
        context.log.error("Error while fetching the HTML")
        return

    path_to_cards = '//*[@class="containerMixitup"]/div/div'
    for item in response.xpath(path_to_cards):
        crawl_item(item, context)
