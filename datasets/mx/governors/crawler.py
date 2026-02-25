import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


# def extract_birth_place_and_date(link_governor_page, context: Context):
#     """
#     The birth place and date can be found in the page of each governor
#     in a ul tag. It is not always in the same position in the list,
#     so we will iterate over all elements and find the one that has
#     the string "Lugar y fecha de nacimiento:". If it is not found,
#     we raise a warning and return None.
#
#     :param link_governor_page: Link to a page of a specific governor
#     :param context: The context object.
#     """
#
#     response = context.fetch_html(link_governor_page)
#
#     path_to_information = '//*[@id="fContenido"]//li'
#
#     for element in response.xpath(path_to_information):
#         text = " ".join(element.itertext())
#         if "Lugar y fecha de nacimiento:" in text:
#             pattern = (
#                 r"Lugar y fecha de nacimiento: ([^.]+\.) (\d{1,2}/\d{1,2}/\d{4})\."
#             )
#             match = re.search(pattern, text)
#             if match:
#                 birth_place = match.group(1)
#                 birth_date = match.group(2)
#                 return birth_place, birth_date
#
#     context.log.info(
#         "Failed to identify birth place and date, link: {}".format(link_governor_page)
#     )
#     return None, None


def crawl_item(input_html, context: Context):
    """
    Creates an entity, a position and a occupancy from the raw HTMLElement.

    :param input_html: The div representing that governor
    :param context: The context object.
    """

    # link_governor_page = input_html.xpath("./div/div/div/div[1]/a/@href")[0]

    # The easiest way to get the data from the HTML is surprisingly from the text of the class
    # All the information in the web site is written there in the form:
    # col-xs-12 col-sm-6 col-md-4 pb15 mix {name of the governor} [{state}] {start_date} {end_date}
    # We will then use a regex to extract the name, state, start_date and end_date

    regex_pattern = r"\[(.*?)\] (\d{2}/\d{2}/\d{4}) (\d{2}/\d{2}/\d{4})"

    match = re.search(regex_pattern, input_html.get("class"))

    if match is None:
        context.log.warning("Unable to extract information from HTML element")

    start_date, end_date = (  # noqa: F841
        match.group(2),
        match.group(3),
    )

    # The names in the card always have a title as the first word, for example,
    # Ing. Carlos Lozano de la Torre, where Ing. stands for Engineer
    # We will add two name propreties, one with the "full" name
    # and other with the clean name.
    raw_title = input_html.xpath("./div/div/div/div[2]/h4/a/text()")[0].strip()
    name = re.sub(r"^([A-Z][a-z]*\.)+ ", "", raw_title)
    state = h.xpath_string(
        input_html, ".//div[@class='media-body escudo']/a/text()[1]"
    ).strip()

    person = context.make("Person")
    person.id = context.make_id(name)

    person.add("name", name)

    # birth_place, birth_date = extract_birth_place_and_date(link_governor_page, context)
    #
    # if birth_place is not None:
    #     person.add("birthPlace", birth_place)
    #     h.apply_date(person, "birthDate", birth_date)

    name_of_position = "Governor of " + state.title()
    position = h.make_position(context, name_of_position, country="mx")
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        # No end_date because the source is no longer being updated, so the only thing
        # we can reliably state is the start date - if the term ended prematurely, we wouldn't know.
        categorisation=categorisation,
    )

    # If occupancy is None, then it is not a PEP
    if occupancy is not None:
        context.emit(person)
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
    for item in h.xpath_elements(response, path_to_cards):
        crawl_item(item, context)
