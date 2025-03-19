"""
# Occasional issues:

## 500 Server Error: SPARQL Request Failed for url: https://datos.cplt.cl/catalogos/infoprobidad/csvdeclaraciones

This happens for a few days, then it goes away again for a few weeks.

We've emailed them about it, but since it comes back from time to time, it's probably
related to how their data grows, and who knows whether someone takes action or it
just fixes itself. Give it a few days.
"""

import re

import orjson
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h
from zavod.archive import dataset_data_path
from zavod.logic.pep import categorise

REGEX_JSON = re.compile(r"var datos =(.+?}]);")
DECLARATION_URL = "https://www.infoprobidad.cl/Declaracion/descargarDeclaracionJSon"


def crawl_row(context: Context, declaration_id: int):
    declaration = context.fetch_json(
        DECLARATION_URL, method="POST", data={"ID": declaration_id}, cache_days=30
    )
    declarant = declaration.pop("Datos_del_Declarante", None)
    if declarant is None:
        context.log.info("Declarant data not available", declaration_id=declaration_id)
        return
    person = context.make("Person")

    declarant_hash = declaration.pop("hashCodeDeclarante", None)
    first_name = declarant.pop("nombre")
    patronymic = declarant.pop("Apellido_Paterno", None)
    matronymic = declarant.pop("Apellido_Materno", None)
    entity = declaration.pop("Datos_Entidad_Por_La_Que_Declara")
    position_name = entity.pop("Cargo_Funcion").pop("nombre")

    if declarant_hash is None:
        person.id = context.make_id(first_name, patronymic, matronymic, position_name)
    else:
        person.id = context.make_slug(declarant_hash)

    h.apply_name(
        person,
        first_name=first_name,
        patronymic=patronymic,
        matronymic=matronymic,
    )
    declaration_url = (
        f"https://www.infoprobidad.cl/Declaracion/Declaracion?ID={declaration_id}"
    )
    person.add("sourceUrl", declaration_url)
    service_entity = entity.pop("Servicio_Entidad")
    position_institution = service_entity.pop("nombre")
    position = h.make_position(
        context, f"{position_name}, {position_institution}", country="cl", lang="spa"
    )

    # 'cargos' can be marked as PEPs for all institutions in cl_info_probidad.yml
    res = context.lookup("positions", position_name)
    if not res:
        context.log.warning(
            f"A new 'Cargo' (post) '{position_name}' was identified",
            position_name=position_name,
        )
        return

    categorisation = categorise(context, position, res.is_pep)

    if not categorisation.is_pep:
        return

    start_date = entity.pop("Fecha_Asuncion_Cargo", "")[:10]
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        categorisation=categorisation,
    )

    if occupancy:
        occupancy.add("sourceUrl", declaration_url)
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(
        declaration,
        [
            "Id_Declaracion",
            "Id_Rectificacion",  # rectification id
            "hashCodeDeclaracion",
            "Fecha_de_la_Declaracion",  # declaration date
            "Periodo",  # Period that this declaration applies to (unsure)
            "Tipo_Declaracion",  # declaration type
            "Ciudad",  # city
            "Region",  # region
            "Comuna",  # commune
            "Pais",  # country
            "Datos_del_Conyuge",  # spouse data
            "Declara_Bienes_Conyuge",  # spouse assets
            "Actividades_Profesionales_Conyuge",  # spouse professional activities
            "Patrimonio_Conyuge",  # spouse heritage
            "Datos_Parientes",  # relatives data
            "Bienes_Inmuebles_Situados_En_Chile",  # real estate in chile
            "Bienes_Inmuebles_Situados_En_Extranjero",  # real estate abroad
            "Instrumentos_Valor_TransableChile",  # tradable instruments in chile
            "Instrumentos_Valor_TransableExtranjero",  # tradable instruments abroad
            "Derechos_Acciones_Chile",  # rights and shares in chile
            "Derechos_Acciones_Extranjero",  # rights and shares abroad
            "Derecho_Aprovechamiento_De_Aguas",  # water use rights
            "Otros_Bienes_Muebles",  # other movable property
            "Contratos",  # contracts
            "Deudas_Pension_Alimentos",  # alimony debts
            "Pasivos",  # liabilities
            "Naves_Artefactos_Navales",  # ships and naval artifacts
            "Aeronaves",  # aircraft
            "Concesiones",  # concessions
            "Otros_Bienes",  # other assets
            "Otros_Antecedentes",  # other data
            "Sujeto_Obligado",  # obligated subject
            "Datos_Personas_Tutela_Curatela",  # guardianship data
            "Vehiculos_Motorizados",  # motor vehicles
            "Actividades_Profesionales_Ultimos_12_Meses",  # professional activities in the last 12 months
            "Actividades_Profesionales_A_La_Fecha_Declaracion",  # professional activities at the declaration date
            "Actividades_Profesionales_A_La_Fecha",  # professional activities at the date
            "Otras_Fuentes",  # other sources
            "Fuente_Conflicto",  # conflict source
            "Patrimonio_Tutela_Curatela",  # guardianship heritage
            "subnumeral",
            "Deposito_Plazo_Adicional",  # additional term deposit
            "Ahorro_Previsional_Voluntario_Adicional",  # additional voluntary pension savings
            "Seguro_Adicional",  # additional insurance
        ],
    )


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.dataset.data.url)
    json_path = dataset_data_path(context.dataset.name) / "source.json"

    with open(path, "r") as fh:
        html = fh.read()
    json = REGEX_JSON.search(html).group(1)
    with open(json_path, "w") as fh:
        fh.write(json)
    context.export_resource(json_path, JSON, title=context.SOURCE_TITLE)

    declarations = orjson.loads(json)
    for declaration in declarations:
        crawl_row(context, declaration.pop("IdDeclaracion"))
