from typing import List
from zavod import Context, helpers as h


def fetch_data(context: Context) -> List[dict]:
    """
    Fetches data from the website, or raises an exception on failure.

    :param context: The context object.
    :return: List of dicts containing data.
    """
    response = context.fetch_json(context.data_url)
    if "items" not in response:
        context.log.error("Items not found in JSON")
        return []
    return response["items"]


def create_entity(input_dict: dict, context: Context):
    """
    Creates an entity from the raw data.

    :param input_dict: Data dict extracted from source.
                       Should have at least following keys:
                        - cpf
                        - nome
    :param context: The context object.
    """

    entity = context.make("Person")
    # the CPF comes in formatted form (i.e. with punctuaction, XXX.XXX.XXX-XX)
    # and it's common practice to remove this punctuation when saving in the database
    raw_tax_number = input_dict["cpf"]
    tax_number = raw_tax_number.replace(".", "").replace("-", "")

    entity.id = context.make_slug(tax_number, prefix="br-cpf")
    entity.add("name", input_dict["nome"])
    entity.add("taxNumber", tax_number)
    entity.add("country", "br")
    entity.add("topics", "debarment")

    return entity


def create_sanction(input_dict: dict, entity, context: Context):
    """
    Creates the sanction for a given entity and the raw data.

    :param input_dict: Data dict extracted from source.
                       Should have at least following keys:
                        - processo
                        - deliberacao
                        - data_transito_julgado
                        - data_final
    :param entity: Entity the sanction refers to.
    :param context: The context object.
    """

    process_number = input_dict.pop("processo")

    sanction = h.make_sanction(context, entity, key=process_number)

    # The ID of the process
    sanction.add(
        "description",
        "Process Number: {}, acording to the deliberation number: {}".format(
            process_number, input_dict.pop("deliberacao")
        ),
    )

    # The start will be defined as the date of the final appel (transito em julgado)
    sanction.add("startDate", input_dict.pop("data_transito_julgado"))
    sanction.add("endDate", input_dict.pop("data_final"))

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
