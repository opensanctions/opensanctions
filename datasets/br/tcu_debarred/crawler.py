from typing import Any

from zavod import Context, helpers as h
from rigour.ids.stdnum_ import CPF, CNPJ


def crawl_item(context: Context, input_dict: dict[str, Any]) -> None:
    """
    Creates an entity from the raw data.

    :param input_dict: Data dict extracted from source.
                       Should have at least following keys:
                        - tipoRegistro
                        - numeroRegistro
                        - nome
                        - numeroProcessoFormatado
                        - numeroAcordaoFormatado
                        - dataTransitoEmJulgado
                        - dataFinalSancao
    :param context: The context object.
    """

    entity = context.make("LegalEntity")

    registry_type = input_dict.pop("tipoRegistro")
    raw_tax_number = input_dict.pop("numeroRegistro")
    name = input_dict.pop("nome")

    if registry_type == "CNPJ":
        entity.add_schema("Company")
        tax_number = (
            CNPJ.normalize(raw_tax_number) if raw_tax_number is not None else None
        )
        prefix = "br-cnpj"
    elif registry_type == "CPF":
        entity.add_schema("Person")
        tax_number = (
            CPF.normalize(raw_tax_number) if raw_tax_number is not None else None
        )
        prefix = "br-cpf"
    else:
        context.log.warning("Unexpected registry type", registry_type=registry_type)
        tax_number = None
        prefix = "br-unknown"

    # If the tax number is missing or invalid, we generate a hash as the id
    if tax_number is None:
        entity.id = context.make_id(raw_tax_number, name)
    else:
        entity.id = context.make_slug(tax_number, prefix=prefix)
    entity.add("name", name)
    entity.add("taxNumber", tax_number)
    entity.add("country", "br")
    entity.add("topics", "debarment")

    process_number = input_dict.pop("numeroProcessoFormatado")

    sanction = h.make_sanction(context, entity, key=process_number)

    # The ID of the process
    sanction.add(
        "description",
        "Process Number: {}, acording to the deliberation number: {}".format(
            process_number, input_dict.pop("numeroAcordaoFormatado")
        ),
    )

    # The start will be defined as the date of the final appel (transito em julgado)
    h.apply_date(sanction, "startDate", input_dict.pop("dataTransitoEmJulgado"))
    h.apply_date(sanction, "endDate", input_dict.pop("dataFinalSancao"))

    sanction.add(
        "program",
        (
            "TCU Debarred entities based on Article 46 of Law 8.443/92 that "
            "restricts entities from participating in public biddings"
        ),
    )
    context.audit_data(
        input_dict,
        ignore=[
            # Decision date
            "dataAcordao",
            # Internal process ID and links to the process and its deliberations
            "codigoProcesso",
            "linkAcompanhamentoProcesso",
            "linkDeliberacoesProcesso",
            # Whether the process concerns public management; always "S"
            "seProcessoGestao",
            # State code
            "uf",
            # Commune
            "municipio",
        ],
    )

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    page = 1
    while True:
        params = {"paginaAtual": page, "tamanhoPagina": 500}
        response = context.fetch_json(context.data_url, params=params, method="POST")
        total_pages = response.get("totalPaginas")
        assert isinstance(total_pages, int), total_pages
        for item in response["elementos"]:
            crawl_item(context, item)

        if page >= total_pages:
            break
        page += 1
