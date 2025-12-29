import os
from typing import Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from zavod.stateful.positions import categorise

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api

SENATORS_URL = "https://www.senado.es/web/composicionorganizacion/senadores/composicionsenado/index.html"
DEPUTIES_URL = "https://www.congreso.es/en/busqueda-de-diputados"
DEPUTIES_API_URL = "https://www.congreso.es/en/busqueda-de-diputados?p_p_id=diputadomodule&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=searchDiputados&p_p_cacheability=cacheLevelPage"
IGNORE = ["constituency_id", "constituency_name", "legislative_term_id", "gender"]


def rename_headers(context: Context, entry: dict[str, str]) -> dict[str, str]:
    result = {}
    for old_key, value in entry.items():
        new_key = context.lookup_value("columns", old_key)
        if new_key is None:
            context.log.warning("Unknown column title", column=old_key)
            new_key = old_key
        result[new_key] = value
    return result


def emit_pep_entities(
    context: Context,
    *,
    person: Entity,
    position_name: str,
    lang: str,
    start_date: Optional[str],
    end_date: Optional[str],
    is_pep: bool,
    wikidata_id: Optional[str] = None,
) -> bool:
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
        return True
    return False


def get_birth_date_and_place(
    context: Context, profile_url: str
) -> tuple[str, str | None]:
    """Get the birth date and place from the profile URL.

    Format looks like one of the following:

    - "Born on 05/12/1976"
    - "Born on 09/03/1975 in Riudoms (Tarragona)"
    """
    born_xpath = "//h3[text() = 'Personal file']"
    doc = zyte_api.fetch_html(
        context, profile_url, unblock_validator=born_xpath, cache_days=15
    )
    born = h.element_text(
        h.xpath_elements(doc, born_xpath, expect_exactly=1)[0].getnext()
    )
    parts = born.split(" in ")
    birth_date = parts[0].replace("Born on", "").strip()
    birth_place = parts[1].strip() if len(parts) > 1 else None
    return birth_date, birth_place


def crawl_deputy(
    context: Context, item: dict[str, str], current_leg_roman: str
) -> bool:
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

    person = context.make("Person")
    person.id = context.make_id(id, name, party)
    h.apply_name(
        person,
        full=name,
        first_name=item.pop("first_name"),
        last_name=item.pop("last_name"),
    )
    birth_date, birth_place = get_birth_date_and_place(context, profile_url)
    h.apply_date(person, "birthDate", birth_date)
    person.add("birthPlace", birth_place)
    person.add("political", party)
    person.add("political", item.pop("parliamentary_group"))
    person.add("sourceUrl", profile_url)
    emitted = emit_pep_entities(
        context,
        person=person,
        position_name="Member of the Congress of Deputies of Spain",
        lang="eng",
        start_date=item.pop("start_date"),
        end_date=item.pop("end_date", None),
        is_pep=True,
        wikidata_id="Q18171345",
    )
    context.audit_data(item, IGNORE)
    return emitted


def crawl_senator(context: Context, senator_url: str) -> bool:
    parsed_url = urlparse(senator_url)
    query_params = parse_qs(parsed_url.query)
    senator_id = query_params["id1"][0]
    legis = query_params["legis"][0]
    xml_url = f"https://www.senado.es/web/ficopendataservlet?tipoFich=1&cod={senator_id}&legis={legis}"
    _, _, _, path = zyte_api.fetch_resource(
        context, filename=f"source_{senator_id}.xml", url=xml_url
    )
    # It looks like some newly-added senators don't have a link to the XML file
    # on their detail page, and the XML url pattern with their ID returns an empty file.
    if os.path.getsize(path) == 0:
        context.log.info(
            "Empty XML file for senator", senator_id=senator_id, url=xml_url
        )
        return False

    doc_xml = context.parse_resource_xml(path)
    datos = doc_xml.find("datosPersonales")
    assert datos is not None
    web_id = datos.findtext("idweb")
    full_first_name = h.element_text(
        h.xpath_elements(datos, "nombre", expect_exactly=1)[0]
    )
    last_name = datos.findtext("apellidos")

    emitted = False
    person = context.make("Person")
    person.id = context.make_id(web_id, full_first_name, last_name)
    h.apply_name(
        person,
        first_name=full_first_name,
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
            emitted |= emit_pep_entities(
                context,
                person=person,
                position_name=f"{role_title}, {role_body}",
                lang="spa",
                start_date=cargo.findtext("cargoAltaFec"),
                end_date=cargo.findtext("cargoBajaFec"),
                is_pep=True,
            )

        if legislatura.findtext("legislaturaActual") == "SI":
            emitted |= emit_pep_entities(
                context,
                person=person,
                position_name="Member of the Senate of Spain",
                lang="eng",
                start_date=None,
                end_date=None,
                is_pep=True,
                wikidata_id="Q19323171",
            )
    return emitted


def crawl(context: Context) -> None:
    listed_deputies = 0
    emitted_deputies = 0
    listed_senators = 0
    emitted_senators = 0

    # Crawl Deputies
    legislature_select_xpath = "//select[@id='_diputadomodule_legislatura']"
    deputies_doc = zyte_api.fetch_html(
        context, DEPUTIES_URL, unblock_validator=legislature_select_xpath, cache_days=1
    )
    leg_select = h.xpath_elements(
        deputies_doc, legislature_select_xpath, expect_exactly=1
    )[0]
    current_leg_option = h.xpath_elements(
        leg_select, "//option[@selected='']", expect_exactly=1
    )[0]
    current_leg_decimal = h.xpath_string(current_leg_option, "@value")
    current_leg_roman = h.element_text(current_leg_option).split(" ")[0].strip()
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
        listed_deputies += 1
        item = rename_headers(context, item)
        if crawl_deputy(context, item, current_leg_roman):
            emitted_deputies += 1

    context.log.info(f"Listed deputies: {listed_deputies}, emitted: {emitted_deputies}")

    # Crawl Senators
    letter_url_xpath = ".//ul[@class='listaOriginal']//@href"
    doc = zyte_api.fetch_html(
        context,
        SENATORS_URL,
        unblock_validator=letter_url_xpath,
        cache_days=1,
        absolute_links=True,
    )
    for letter_url in h.xpath_strings(doc, letter_url_xpath):
        letter_doc = zyte_api.fetch_html(
            context,
            letter_url,
            # div of the main list, which may be empty for some letters
            unblock_validator="//div[@class='caja12']",
            absolute_links=True,
        )
        for senator_href in h.xpath_strings(
            letter_doc, ".//ul[@class='lista-alterna']//@href"
        ):
            listed_senators += 1

            # Sometimes, absolute_links=True doesn't work (if lxml doesn't want to parse as HTML)
            # urljoin will do the right thing in all cases (if senator_href contains a full URL
            # or just a path)
            senator_url = urljoin(letter_url, senator_href)
            if crawl_senator(context, senator_url):
                emitted_senators += 1

    context.log.info(f"Listed senators: {listed_senators}, emitted: {emitted_senators}")
