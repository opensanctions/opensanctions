from typing import Any

from rigour.ids.stdnum_ import CPF

from zavod import Context
from zavod import helpers as h


def crawl_item(input_dict: dict[str, Any], context: Context) -> None:
    """
    Creates an entity from the raw data.

    :param input_dict: Data dict extracted from source.
                       Should have at least following keys:
                        - cpf
                        - nome
                        - processo
                        - deliberacao
                        - data_transito_julgado
                        - data_final
    :param context: The context object.
    """

    entity = context.make("Person")

    cpf = input_dict.pop("cpf")
    name = input_dict.pop("nome")
    # make sure the CPF does not have any punctuation
    cleaned_tax_number = CPF.normalize(cpf)

    # if the tax number is None, it means it was invalid
    if cleaned_tax_number is None:
        entity.id = context.make_id(cpf, name)
    else:
        entity.id = context.make_slug(cleaned_tax_number, prefix="br-cpf")

    entity.add("name", name)
    entity.add("taxNumber", cleaned_tax_number)
    entity.add("country", "br")
    entity.add("topics", "corp.disqual")

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

    sanction.add(
        "program",
        """TCU Disqualified individuals based on Article 60 of Law 8.443/92 that
                               restricts individuals from holding certain influential
                               and sensitive positions in the public sector""",
    )
    context.audit_data(
        input_dict,
        ignore=[
            # Decision date
            "data_acordao",
            # State code
            "uf",
            # Commune
            "municipio",
        ],
    )

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    url = context.data_url
    while True:
        response = context.fetch_json(url)
        if "items" not in response:
            context.log.error("Items not found in JSON")
            return

        for item in response["items"]:
            crawl_item(item, context)

        # if hasMore = true -> there is a link for next
        has_more = response.get("hasMore", False)
        if not has_more:
            break

        links = response.get("links", [])
        link = [li.get("href") for li in links if li.get("rel") == "next"]
        if not len(link):
            break
        url = link[0]
        context.log.info(f"Fetching next page: {url}")
