from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


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


def crawl_item(context, item):
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


def crawl(context: Context):
    data = context.fetch_json(
        "https://www.congreso.es/en/busqueda-de-diputados?p_p_id=diputadomodule&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=searchDiputados&p_p_cacheability=cacheLevelPage",
        method="POST",
        cache_days=1,
        headers=HEADERS,
        data=FORM_DATA,
    )
    for item in data["data"]:
        item = rename_headers(context, item)
        crawl_item(context, item)
