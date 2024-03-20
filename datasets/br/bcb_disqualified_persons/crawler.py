from typing import List
from zavod import Context, helpers as h


def fetch_data(context: Context) -> List[dict]:
    """
    Fetches data from the website, or raises an exception on failure.

    :param context: The context object.
    :return: List of dicts containing data.
    """
    response = context.fetch_json(context.data_url)
    if "value" not in response:
        context.log.error("Value not found in JSON")
        return []
    return response["value"]


def create_entity(input_dict: dict, context: Context):
    """
    Creates an entity from the raw data.

    :param input_dict: Data dict extracted from source.
                       Should have at least following keys:
                        - CPF
                        - Nome
    :param context: The context object.
    """
    entity = context.make("Person")
    tax_number = input_dict["CPF"]
    entity.id = context.make_slug(tax_number, prefix="br-cpf")
    entity.add("name", input_dict["Nome"])
    entity.add("taxNumber", tax_number)
    entity.add("country", "br")
    entity.add("topics", "corp.disqual")

    return entity


def create_sanction(input_dict: dict, entity, context: Context):
    """
    Creates the sanction for a given entity and the raw data.

    :param input_dict: Data dict extracted from source.
                       Should have at least following keys:
                        - PAS
                        - Prazo_em_ano
                        - Inicio_do_cumprimento
                        - Prazo_final_penalidade
    :param entity: Entity the sanction refers to.
    :param context: The context object.
    """

    pas_number = input_dict.pop("PAS")

    sanction = h.make_sanction(context, entity, key=pas_number)

    # The ID of the process
    sanction.add(
        "description",
        "Administrative Sanctioning Process Number: {}".format(pas_number),
    )

    # The duration is always in years
    sanction.add("duration", "{} year(s)".format(input_dict.pop("Prazo_em_anos")))

    # The start and end dates are in the format YYYY-MM-DD
    sanction.add("startDate", input_dict.pop("Inicio_do_cumprimento"))
    sanction.add("endDate", input_dict.pop("Prazo_final_penalidade"))

    return sanction


def crawl(context: Context):
    """
    Entrypoint to the crawler.

    The crawler works by fetching the data from the URL as a JSON.
    The data is already in the format of a list of dicts, so we just need to create the entities.

    :param context: The context object.
    """
    data = fetch_data(context)
    for line in data:
        entity = create_entity(line, context)
        sanction = create_sanction(line, entity, context)
        context.emit(entity, target=True)
        context.emit(sanction)
