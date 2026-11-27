from typing import Any

from rigour.ids.stdnum_ import CPF

from zavod import Context
from zavod import helpers as h


def crawl_item(input_dict: dict[str, Any], context: Context) -> None:
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

    entity = context.make("Person")

    registry_type = input_dict.pop("tipoRegistro")
    if registry_type != "CPF":
        context.log.warning("Unexpected registry type", registry_type=registry_type)
    cpf = input_dict.pop("numeroRegistro")
    name = input_dict.pop("nome")
    # make sure the CPF does not have any punctuation
    cleaned_tax_number = CPF.normalize(cpf) if cpf is not None else None

    # if the tax number is None, it means it was missing or invalid
    if cleaned_tax_number is None:
        entity.id = context.make_id(cpf, name)
    else:
        entity.id = context.make_slug(cleaned_tax_number, prefix="br-cpf")

    entity.add("name", name)
    entity.add("taxNumber", cleaned_tax_number)
    entity.add("country", "br")
    entity.add("topics", "corp.disqual")

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
        """TCU Disqualified individuals based on Article 60 of Law 8.443/92 that
                               restricts individuals from holding certain influential
                               and sensitive positions in the public sector""",
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
            crawl_item(item, context)

        if page >= total_pages:
            break
        page += 1
