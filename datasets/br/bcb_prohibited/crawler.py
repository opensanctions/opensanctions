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
    entity = context.make("LegalEntity")
    tax_number = input_dict["CPF_CNPJ"]

    # If it's 14 digits, then it is a CNPJ: https://en.wikipedia.org/wiki/CNPJ
    if len(tax_number) == 14:
        entity.add_schema("Company")

    # If it's 11 digits, then it is a CPF: https://en.wikipedia.org/wiki/CPF_number
    elif len(tax_number) == 11:
        entity.add_schema("Person")

    # It cannot be anything other than those two
    else:
        context.log.error("Tax number is neither a CPF nor CNPJ")

    entity.id = context.make_id(tax_number)
    entity.add("name", input_dict["Nome"])
    entity.add("taxNumber", tax_number)
    entity.add("country", "br")
    entity.add('topics', 'debarment')

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

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "program", "Brazil's Central Bank General Register of Persons and Companies Prohibited from Offering Auditing Services"
    )
    sanction.add("authority", "Brazil's Central Bank")

    # The ID of the process
    sanction.add(
        "description",
        "Administrative Sanctioning Process Number: {}".format(input_dict.pop("PAS")),
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
