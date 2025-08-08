import os
import string
from urllib.parse import urlparse, parse_qs

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


SENATORS_URL = "https://www.senado.es/web/composicionorganizacion/senadores/composicionsenado/consultaordenalfabetico/index.html"
DEPUTIES_URL = "https://www.congreso.es/en/busqueda-de-diputados?p_p_id=diputadomodule&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=searchDiputados&p_p_cacheability=cacheLevelPage"
FORM_DATA = {
    "_diputadomodule_idLegislatura": "15",
    "_diputadomodule_genero": "0",
    "_diputadomodule_grupo": "all",
    "_diputadomodule_tipo": "0",
    "_diputadomodule_formacion": "all",
    "_diputadomodule_filtroProvincias": "[]",
}
IGNORE = ["constituency_id", "constituency_name", "legislative_term_id", "gender"]


def rename_headers(context, entry):
    result = {}
    for old_key, value in entry.items():
        new_key = context.lookup_value("columns", old_key)
        if new_key is None:
            context.log.warning("Unknown column title", column=old_key)
            new_key = old_key
        result[new_key] = value
    return result


def emit_pep_entities(
    context,
    person,
    title,
    lang,
    start_date,
    end_date,
    is_pep,
    wikidata_id=None,
):
    person.add("position", title)
    position = h.make_position(
        context,
        name=title,
        country="es",
        lang=lang,
        topics=["gov.legislative", "gov.national"],
        wikidata_id=wikidata_id,
    )
    categorisation = categorise(context, position, is_pep=is_pep)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy:
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)


def crawl_deputy(context, item):
    name = item.pop("full_name")
    id = item.pop("parliament_member_id")
    party = item.pop("party")

    person = context.make("Person")
    person.id = context.make_id(id, name, party)
    first_names = item.pop("first_name").split(" ", 1)
    h.apply_name(
        person,
        full=name,
        first_name=first_names[0],
        second_name=first_names[1] if len(first_names) > 1 else None,
        last_name=item.pop("last_name"),
    )
    person.add("political", party)
    person.add("political", item.pop("parliamentary_group"))
    emit_pep_entities(
        context,
        person,
        "Member of the Congress of Deputies of Spain",
        "eng",
        item.pop("start_date"),
        item.pop("end_date", None),
        is_pep=True,
        wikidata_id="Q18171345",
    )
    context.audit_data(item, IGNORE)


def crawl_senator(context, doc_xml, link):
    datos = doc_xml.find("datosPersonales")
    legislatura = doc_xml.find(".//legislatura")
    credencial = legislatura.find(".//credencial") if legislatura is not None else None
    grupo = legislatura.find(".//grupoParlamentario")
    web_id = datos.findtext("idweb")
    first_names = datos.findtext("nombre").split(" ", 1)
    last_name = datos.findtext("apellidos")

    person = context.make("Person")
    person.id = context.make_id(web_id, first_names, last_name)
    h.apply_name(
        person,
        first_name=first_names[0],
        second_name=first_names[1] if len(first_names) > 1 else None,
        last_name=last_name,
    )
    h.apply_date(person, "birthDate", datos.findtext("fechaNacimiento"))
    h.apply_date(person, "deathDate", datos.findtext("fechaFallecimiento"))
    person.add("birthPlace", datos.findtext("lugarNacimiento"))
    if credencial is not None or grupo is not None:
        person.add("political", credencial.findtext("partidoSiglas"))
        person.add("political", grupo.findtext("grupoNombre"))
    person.add("notes", datos.findtext("biografia"))
    person.add("sourceUrl", link)
    emit_pep_entities(
        context,
        person,
        "Member of the Senate of Spain",
        "eng",
        grupo.findtext("grupoAltaFec"),
        grupo.findtext("grupoBajaFec"),
        is_pep=True,
        wikidata_id="Q19323171",
    )
    # Parliamentary roles (cargos)
    for cargo in legislatura.findall(".//cargo"):
        role_title = cargo.findtext("cargoNombre")
        role_body = cargo.findtext("cargoOrganoNombre")
        emit_pep_entities(
            context,
            person,
            f"{role_title}, {role_body}",
            "spa",
            cargo.findtext("cargoAltaFec"),
            cargo.findtext("cargoBajaFec"),
            # there are a lot of parliamentary postions, do we want to go into the details?
            # example: MEMBER OF THE COMMITTEE ON EDUCATION, VOCATIONAL TRAINING, AND SPORTS
            is_pep=True,
        )


def crawl(context: Context):
    # Crawl Deputies
    data = context.fetch_json(
        DEPUTIES_URL,
        method="POST",
        cache_days=1,
        headers={"Accept": "application/json"},
        data=FORM_DATA,
    )
    for item in data["data"]:
        item = rename_headers(context, item)
        crawl_deputy(context, item)

    # Crawl Senators
    for letter in string.ascii_uppercase:
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
            if os.path.getsize(path) == 0:
                context.log.warn("Empty XML file", url=xml_url)
                continue
            doc_xml = context.parse_resource_xml(path)
            crawl_senator(context, doc_xml, link)
