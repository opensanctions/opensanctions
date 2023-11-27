from normality import slugify
from typing import Optional, List

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

FORMATS = ["%d/%m/%Y", "%d/%m/%y"]
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
    table = doc.find('.//table[@id="table_1"]')
    assert table is not None
    headers: Optional[List[str]] = None
    for row in table.findall(".//tr"):
        if headers is None and len(row.findall(".//th")):
            headers = [slugify(c.text, sep="_") for c in row.findall(".//th")]
            continue

        cells = [c.text for c in row.findall("./td")]
        data = dict(zip(headers, cells))
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
        dob = h.parse_date(data.pop("fecha_de_nacimiento"), FORMATS)
        entity.add("birthDate", dob)
        entity.add("nationality", "cu")
        entity.add("topics", "role.pep")

        occupancy = h.make_occupancy(
            context,
            entity,
            position,
            start_date=inception_date,
            end_date=dissolution_date,
            no_end_implies_current=True,
            categorisation=categorisation,
        )
        context.emit(occupancy)

        context.audit_data(data, ignore=IGNORE_COLUMNS)
        context.emit(entity, target=True)
