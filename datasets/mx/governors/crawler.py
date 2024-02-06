import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

def extract_birth_place_and_date(link_governor_page, context: Context):
    """
    The birth place and date can be found in the page of each governor
    in a ul tag. It is not always in the same position in the list,
    so we will iterate over all elements and find the one that has
    the string "Lugar y fecha de nacimiento:". If it is not found,
    we raise a warning and return None.

    :param link_governor_page: Link to a page of a specific governor
    :param context: The context object.
    """

    response = context.fetch_html(link_governor_page)

    path_to_information = '//*[@id="fContenido"]/div/div/div[2]/div/div/div/div[2]/div/div[1]/ul/li'

    for element in response.xpath(path_to_information):
        text = ' '.join(element.itertext())
        if 'Lugar y fecha de nacimiento:' in text:
            pattern = r'Lugar y fecha de nacimiento: ([^.]+\.) (\d{1,2}/\d{1,2}/\d{4})\.'
            match = re.search(pattern, text)
            if match:
                birth_place = match.group(1)
                birth_date = match.group(2)
                return birth_place, birth_date

    context.log.warning("Failed to identify birth place and date, link: {}".format(link_governor_page))
    return None, None


def crawl_item(input_html, link_governor_page, context: Context):
    """
    Creates an entity, a position and a occupancy from the raw HTMLElement.

    :param input_html: The div representing that governor
    :param link_governor_page: Link to that governor page
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

    # The names in the card always have a title as the first word, for example,
    # Ing. Carlos Lozano de la Torre, where Ing. stands for Engineer
    # We will add two name propreties, one with the "full" name
    # and other with the clean name. The clean name
    # can be retrived from the url to that governors page
    person.add("name", name)
    clean_name = (link_governor_page
                  .replace("https://www.conago.org.mx/miembros/detalle/", "")
                  .replace("-", " ")
                  .title())
    person.add("name", clean_name)

    birth_place, birth_date = extract_birth_place_and_date(link_governor_page, context)

    if birth_place is not None:
        person.add("birthPlace", birth_place)
        person.add("birthDate", h.parse_date(birth_date, formats=["%d/%m/%Y"])[0])

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


    # If occupancy is None, then it is not a PEP
    if occupancy is not None:
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
    path_to_links = '//*[@class="containerMixitup"]/div/div/div/div/div/div[1]/a'
    for (item, a_tag) in zip(response.xpath(path_to_cards), response.xpath(path_to_links)):
        crawl_item(item, a_tag.get('href'), context)
