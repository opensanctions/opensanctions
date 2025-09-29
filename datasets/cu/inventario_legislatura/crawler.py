from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h

IGNORE_COLUMNS = [
    "edad",
    "votos_validos",
    "nivel_de_su_cargo",
    "vinculacion_laboral",
    "provincia",
    "municipio",
    "diputado_en_9na_legislatura",
    "fuente_fecha_de_nacimiento",
    "rango_de_edad",
    "edad_informada",
    "comprobacion_edad",
    "distrito",
    "miembro_del_cc",
    "miembro_del_buro_politico",
    "vinculo_far_minint",
    "fuente_votos",
    "link_a_imagen_en_drive",
    "twitter",
    "link_twitter",
    "facebook",
    "link_facebook",
]


def crawl(context: Context):
    pos = "X Legislatura de la Asamblea Nacional del Poder Popular (ANPP)"
    inception_date = "2023"
    dissolution_date = "2028"
    position = h.make_position(
        context,
        pos,
        country="cu",
        inception_date=inception_date,
        dissolution_date=dissolution_date,
    )
    categorisation = categorise(context, position, True)
    context.emit(position)

    doc = context.fetch_html(context.data_url)
    tables = doc.findall('.//table[@id="table_1"]')
    assert len(tables) == 1, len(tables)
    for row in h.parse_html_table(tables[0]):
        data = h.cells_to_str(row)
        seat_nr = data.pop("escano")
        if seat_nr is None:
            continue
        name = data.pop("nombre_y_apellidos")

        entity = context.make("Person")
        entity.id = context.make_id(seat_nr, name)
        entity.add("name", name)
        entity.add("notes", data.pop("biografia"))
        entity.add("notes", data.pop("notas"))
        entity.add("gender", data.pop("genero"))
        entity.add("position", data.pop("ocupacion"))
        entity.add("education", data.pop("nivel_escolar"))
        entity.add("political", data.pop("ujc_pcc"))
        h.apply_date(entity, "birthDate", data.pop("fecha_de_nacimiento"))
        entity.add("nationality", "cu")
        entity.add("topics", "role.pep")

        occupancy = h.make_occupancy(
            context,
            entity,
            position,
            start_date=inception_date,
            end_date=None,
            no_end_implies_current=False,
            categorisation=categorisation,
        )
        context.emit(occupancy)

        context.audit_data(data, ignore=IGNORE_COLUMNS)
        context.emit(entity)
