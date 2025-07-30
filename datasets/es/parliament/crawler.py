from urllib.parse import urlparse, parse_qs

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


SENATORS_URL = "https://www.senado.es/web/composicionorganizacion/senadores/composicionsenado/consultaordenalfabetico/index.html"
DEPUTIES_URL = "https://www.congreso.es/en/busqueda-de-diputados?p_p_id=diputadomodule&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=searchDiputados&p_p_cacheability=cacheLevelPage"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
}
FORM_DATA = {
    "_diputadomodule_idLegislatura": "15",
    "_diputadomodule_genero": "0",
    "_diputadomodule_grupo": "all",
    "_diputadomodule_tipo": "0",
    "_diputadomodule_nombre": "",
    "_diputadomodule_apellidos": "",
    "_diputadomodule_formacion": "all",
    "_diputadomodule_filtroProvincias": "[]",
    "_diputadomodule_nombreCircunscripcion": "",
}
IGNORE = ["constituency_id", "legislative_term_id", "gender"]


def rename_headers(context, entry):
    result = {}
    for old_key, value in entry.items():
        new_key = context.lookup_value("columns", old_key)
        if new_key is None:
            context.log.warning("Unknown column title", column=old_key)
            new_key = old_key
        result[new_key] = value
    return result


def emit_parliamentary_position(context, pep, cargo):
    role_title = cargo.findtext("cargoNombre")
    role_body = cargo.findtext("cargoOrganoNombre")
    role_start_date = cargo.findtext("cargoAltaFec")
    role_end_date = cargo.findtext("cargoBajaFec")
    position = h.make_position(
        context,
        name=f"{role_title}, {role_body}",
        country="es",
        lang="spa",
        topics=["gov.legislative", "gov.national"],
    )
    context.emit(position)
    categorisation = categorise(context, position, is_pep=True)
    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        start_date=role_start_date,
        end_date=role_end_date,
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(occupancy)
        context.emit(position)


def crawl_deputy(context, item):
    name = item.pop("full_name")
    id = item.pop("parliament_member_id")
    party = item.pop("party")

    pep = context.make("Person")
    pep.id = context.make_id(id, name, party)
    h.apply_name(
        pep,
        full=name,
        first_name=item.pop("first_name"),
        last_name=item.pop("last_name"),
    )
    pep.add("political", party)
    pep.add("political", item.pop("parliamentary_group"))
    pep.add("topics", "role.pep")

    position = h.make_position(
        context,
        name="Member of Parliament",
        country="es",
        subnational_area=item.pop("constituency_name"),
        lang="spa",
        topics=["gov.legislative", "gov.national"],
    )
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        start_date=item.pop("start_date"),
        end_date=item.pop("end_date", None),
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(occupancy)
        context.emit(position)
        context.emit(pep)

    context.audit_data(item, IGNORE)


def crawl_senator(context, doc_xml, link):
    datos = doc_xml.find("datosPersonales")
    legislatura = doc_xml.find(".//legislatura")
    credencial = legislatura.find(".//credencial") if legislatura is not None else None
    grupo = legislatura.find(".//grupoParlamentario")

    web_id = datos.findtext("idweb")
    first_name = datos.findtext("nombre")
    last_name = datos.findtext("apellidos")
    dob = datos.findtext("fechaNacimiento")
    pob = datos.findtext("lugarNacimiento")
    biography = datos.findtext("biografia")
    date_of_death = datos.findtext("fechaFallecimiento")

    if credencial is not None:
        party_acronym = credencial.findtext("partidoSiglas")
        election_date = credencial.findtext("procedFecha")
        # party_name = credencial.findtext("partidoNombre")
        # origin = credencial.findtext("procedLiteral")

    if grupo is not None:
        parliamentary_group = grupo.findtext("grupoNombre")

    pep = context.make("Person")
    pep.id = context.make_id(web_id, first_name, last_name)
    h.apply_name(pep, first_name=first_name, last_name=last_name)
    h.apply_date(pep, "birthDate", dob)
    h.apply_date(pep, "deathDate", date_of_death)
    pep.add("birthPlace", pob)
    pep.add("political", party_acronym)
    pep.add("political", parliamentary_group)
    pep.add("notes", biography)
    pep.add("sourceUrl", link)
    pep.add("topics", "role.pep")

    position = h.make_position(
        context,
        name="Member of Parliament",
        country="es",
        lang="spa",
        topics=["gov.legislative", "gov.national"],
    )
    # Parliamentary roles (cargos)
    for cargo in legislatura.findall(".//cargo"):
        emit_parliamentary_position(context, pep, cargo)

    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        start_date=grupo.findtext("grupoAltaFec"),
        end_date=grupo.findtext("grupoBajaFec"),
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(occupancy)
        context.emit(position)
        context.emit(pep)


def crawl(context: Context):
    # Crawl Deputies
    data = context.fetch_json(
        DEPUTIES_URL,
        method="POST",
        cache_days=1,
        headers=HEADERS,
        data=FORM_DATA,
    )
    for item in data["data"]:
        item = rename_headers(context, item)
        crawl_deputy(context, item)

    # Crawl Senators
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        url = f"{SENATORS_URL}?id={letter}"
        doc = context.fetch_html(url, cache_days=1)
        doc.make_links_absolute(url)
        senator_links = doc.xpath(".//ul[@class='lista-alterna']//@href")
        for link in senator_links:
            parsed_url = urlparse(link)
            query_params = parse_qs(parsed_url.query)
            senator_id = query_params["id1"][0]
            legis = query_params["legis"][0]
            xml_url = f"https://www.senado.es/web/ficopendataservlet?tipoFich=1&cod={senator_id}&legis={legis}"
            path = context.fetch_resource(f"source_{senator_id}.xml", xml_url)
            doc_xml = context.parse_resource_xml(path)
            crawl_senator(context, doc_xml, link)
