from zavod import Context, helpers as h
from rigour.ids.stdnum_ import CPF, CNPJ


def crawl_item(input_dict: dict, context: Context):
    """
    Creates an entity from the raw data.

    :param input_dict: Data dict extracted from source.
                       Should have at least following keys:
                        - cpf_cnpj
                        - nome
                        - processo
                        - deliberacao
                        - data_transito_julgado
                        - data_final
    :param context: The context object.
    """

    entity = context.make("LegalEntity")

    raw_tax_number = input_dict["cpf_cnpj"]

    # If it's length 18 with punctuation, then it is a CNPJ (XX.XXX.XXX/XXXX-XX)
    if len(raw_tax_number) == 18:
        entity.add_schema("Company")
        tax_number = CNPJ.normalize(raw_tax_number)
        prefix = "br-cnpj"

    # If it's length 14 with punctuation, then it is a CPF (XXX.XXX.XXX-XX)
    elif len(raw_tax_number) == 14:
        entity.add_schema("Person")
        tax_number = CPF.normalize(raw_tax_number)
        prefix = "br-cpf"

    # If it's neither, then we just use the raw number
    else:
        context.log.warning("Entity type not defined by tax number")
        tax_number = None
        prefix = "br-unknown"

    # If the tax number couldn't be defined as either a CPF or CNPJ, we generate a hash as the id
    if tax_number is None:
        entity.id = context.make_id(input_dict["cpf"], input_dict["nome"])
    else:
        entity.id = context.make_slug(tax_number, prefix=prefix)
    entity.add("name", input_dict["nome"])
    entity.add("taxNumber", tax_number)
    entity.add("country", "br")
    entity.add("topics", "debarment")

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
        (
            "TCU Debarred entities based on Article 46 of Law 8.443/92 that "
            "restricts entities from participating in public biddings"
        ),
    )

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    url = context.data_url
    while True:
        response = context.fetch_json(url)
        if "items" not in response:
            context.log.error("Items not found in JSON")
            return

        for item in response["items"]:
            crawl_item(item, context)

        has_more = response.get("hasMore", False)
        if not has_more:
            break

        links = response.get("links", [])
        link = [li.get("href") for li in links if li.get("rel") == "next"]
        if not len(link):
            break
        url = link[0]
        context.log.info(f"Fetching next page: {url}")
