import os
from urllib.parse import parse_qs, urlencode, urlparse

from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h

SENATORS_URL = "https://www.senado.es/web/composicionorganizacion/senadores/composicionsenado/index.html"
DEPUTIES_URL = "https://www.congreso.es/en/busqueda-de-diputados"
DEPUTIES_API_URL = "https://www.congreso.es/en/busqueda-de-diputados?p_p_id=diputadomodule&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=searchDiputados&p_p_cacheability=cacheLevelPage"
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
    position_name,
    lang,
    start_date,
    end_date,
    is_pep,
    wikidata_id=None,
):
    person.add("position", position_name)
    position = h.make_position(
        context,
        name=position_name,
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


def get_birth_date_and_place(context, profile_url):
    doc = context.fetch_html(profile_url, cache_days=15)
    born = doc.xpath("//h3[text() = 'Personal file']")[0].getnext().text_content()
    parts = born.split(" in ")
    birth_date = parts[0].replace("Born on", "").strip()
    # very rough sanity check for date
    assert "00:00:00 CE" in birth_date, birth_date
    birth_place = parts[1].strip() if len(parts) > 1 else None
    return birth_date, birth_place


def crawl_deputy(context, item, current_leg_roman):
    id = item.pop("parliament_member_id")
    query = {
        "p_p_id": "diputadomodule",
        "p_p_lifecycle": "0",
        "p_p_state": "normal",
        "p_p_mode": "view",
        "_diputadomodule_mostrarFicha": "true",
        "codParlamentario": id,
        "idLegislatura": current_leg_roman,
    }
    profile_url = f"{DEPUTIES_URL}?{urlencode(query)}"

    name = item.pop("full_name")
    party = item.pop("party")
    birth_date, birth_place = get_birth_date_and_place(context, profile_url)

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
    birth_date = birth_date.split(" ", 1)[1]
    birth_date = birth_date.replace("CEST", "CET")
    birth_date = birth_date.replace("00:00:00 CET", "")
    h.apply_date(person, "birthDate", birth_date)
    person.add("birthPlace", birth_place)
    person.add("political", party)
    person.add("political", item.pop("parliamentary_group"))
    person.add("sourceUrl", profile_url)
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


def crawl_senator(context, senator_url):
    parsed_url = urlparse(senator_url)
    query_params = parse_qs(parsed_url.query)
    senator_id = query_params["id1"][0]
    legis = query_params["legis"][0]
    xml_url = f"https://www.senado.es/web/ficopendataservlet?tipoFich=1&cod={senator_id}&legis={legis}"
    path = context.fetch_resource(f"source_{senator_id}.xml", xml_url)
    # It looks like some newly-added senators don't have a link to the XML file
    # on their detail page, and the XML url pattern with their ID returns an empty file.
    if os.path.getsize(path) == 0:
        return

    doc_xml = context.parse_resource_xml(path)
    datos = doc_xml.find("datosPersonales")
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
    person.add("notes", datos.findtext("biografia"))
    person.add("sourceUrl", senator_url)

    for legislatura in doc_xml.findall(".//legislatura"):
        # Additional parliamentary roles (cargos)
        for cargo in legislatura.findall(".//cargo"):
            role_title = cargo.findtext("cargoNombre")
            role_body = cargo.findtext("cargoOrganoNombre")
            emit_pep_entities(
                context,
                person=person,
                position_name=f"{role_title}, {role_body}",
                lang="spa",
                start_date=cargo.findtext("cargoAltaFec"),
                end_date=cargo.findtext("cargoBajaFec"),
                is_pep=True,
            )

        if legislatura.findtext("legislaturaActual") == "SI":
            emit_pep_entities(
                context,
                person=person,
                position_name="Member of the Senate of Spain",
                lang="eng",
                start_date=None,
                end_date=None,
                is_pep=True,
                wikidata_id="Q19323171",
            )


def crawl(context: Context):
    # Crawl Deputies
    deputies_doc = context.fetch_html(DEPUTIES_URL, cache_days=1)
    leg_select = deputies_doc.xpath("//select[@id='_diputadomodule_legislatura']")[0]
    current_leg_option = leg_select.xpath("//option[@selected='selected']")[0]
    current_leg_decimal = current_leg_option.xpath("@value")[0]
    current_leg_roman = current_leg_option.text_content().split(" ")[0].strip()
    form_data = {
        "_diputadomodule_idLegislatura": current_leg_decimal,
        "_diputadomodule_genero": "0",
        "_diputadomodule_grupo": "all",
        "_diputadomodule_tipo": "0",
        "_diputadomodule_formacion": "all",
        "_diputadomodule_filtroProvincias": "[]",
    }
    data = context.fetch_json(
        DEPUTIES_API_URL,
        method="POST",
        cache_days=1,
        headers={"Accept": "application/json"},
        data=form_data,
    )
    for item in data["data"]:
        item = rename_headers(context, item)
        crawl_deputy(context, item, current_leg_roman)

    # Crawl Senators
    doc = context.fetch_html(SENATORS_URL, cache_days=1, absolute_links=True)
    for letter_url in doc.xpath(".//ul[@class='listaOriginal']//@href"):
        letter_doc = context.fetch_html(letter_url, absolute_links=True)
        for senator_url in letter_doc.xpath(".//ul[@class='lista-alterna']//@href"):
            crawl_senator(context, senator_url)
