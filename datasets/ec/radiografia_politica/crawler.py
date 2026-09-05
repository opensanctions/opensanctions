import csv
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, TypedDict

from lxml.html import fragment_fromstring
from normality import ascii_text
from rigour.mime.types import CSV
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h


class PersonEnrichment(TypedDict):
    birth_date: str | None
    gender: str | None


TWITTER_HANDLE = re.compile(r"^[A-Za-z0-9_]{1,15}$")


def optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def twitter_url(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if value.startswith(("https://", "http://")):
        return value
    handle = ascii_text(value.removeprefix("@"))
    assert TWITTER_HANDLE.fullmatch(handle), f"Unexpected Twitter value: {value!r}"
    return f"https://x.com/{handle}"


def iter_current_rows(path: Path, config: dict[str, Any]) -> Iterator[dict[str, str]]:
    """Read the current-officials CSV after its six-row metadata preamble."""
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)

        metadata_heading = next(reader)
        assert metadata_heading[0] == "Metadatos", metadata_heading

        contact = next(reader)
        assert contact[0] == "Email contacto datos abiertos:", contact

        author = next(reader)
        assert author[:2] == [
            "Autor datos abiertos:",
            "Fundación Ciudadanía y Desarrollo",
        ], author

        license_row = next(reader)
        assert license_row[0] == "Licencia datos abiertos:", license_row
        assert license_row[1] == config["expected_license"], (
            f"Unexpected data license: {license_row[1]!r}"
        )

        separator = next(reader)
        assert all(cell == "" for cell in separator), separator

        data_heading = next(reader)
        assert data_heading[0] == "Datos", data_heading

        rows = csv.DictReader(fh)
        assert rows.fieldnames == config["expected_columns"], rows.fieldnames
        yield from rows


def build_enrichment_index(
    context: Context, payload: dict[str, Any]
) -> dict[str, PersonEnrichment]:
    """Index optional birth dates and gender values by stable source person ID."""
    metadata = payload.pop("metaDatos")
    assert isinstance(metadata, dict), type(metadata)
    assert metadata.pop("licencia") == context.dataset.config["expected_license"], (
        "Unexpected data license in enrichment API"
    )
    context.audit_data(
        metadata,
        ignore=[
            "email_contacto_datos_abiertos",
            "autor_datos_abiertos",
            "key_word",
        ],
    )

    records = payload.pop("datosGeneroPersona")
    assert isinstance(records, list), type(records)
    context.audit_data(payload, ignore=[])

    enrichments: dict[str, PersonEnrichment] = {}
    for raw_record in records:
        assert isinstance(raw_record, dict), type(raw_record)
        record = dict(raw_record)
        source_id = str(record.pop("persona_id"))
        assert str(record.pop("id")) == source_id, source_id
        enrichment = PersonEnrichment(
            birth_date=optional_string(record.pop("fecha_nacimiento")),
            gender=optional_string(record.pop("genero_persona")),
        )
        previous = enrichments.get(source_id)
        assert previous is None or previous == enrichment, (
            f"Conflicting enrichment for person {source_id}"
        )
        enrichments[source_id] = enrichment
        context.audit_data(
            record,
            ignore=[
                "imagen_persona",
                "nombres_persona",
                "apellidos_persona",
                "descripcion_corta_persona",
                "descripcion_persona",
                "plan_persona",
                "twitter_persona",
                "facebook_persona",
                "candidato_persona",
                "observatorio_persona",
                "curriculum_persona",
                "user_id",
                "estado_id",
                "partido_politico_id",
                "created_at",
                "updated_at",
                "deleted_at",
                "visitas",
                "likes",
                "ranking",
                "partidos_politicos_anteriores",
                "cargo",
                "funcion",
                "categoria",
            ],
        )
    return enrichments


def crawl_row(
    context: Context,
    row: dict[str, str],
    enrichments: dict[str, PersonEnrichment],
) -> None:
    source_id = row.pop("id")
    assert source_id != "", row

    role = row.pop("cargo").strip()
    role_description_html = row.pop("descripcion_cargo").strip()
    role_description = ""
    if role_description_html:
        role_description = h.element_text(
            fragment_fromstring(role_description_html, create_parent=True)
        )
    position_name = role_description or role
    assert position_name != "", source_id

    status = row.pop("estado_cargo")
    assert status == "Funcionario", f"Unexpected office status: {status!r}"

    first_name = row.pop("name")
    last_name = row.pop("lastname")
    party = row.pop("partido")
    facebook = row.pop("facebook").strip()
    if facebook:
        assert facebook.startswith(("https://", "http://")), (
            f"Unexpected Facebook value: {facebook!r}"
        )
    twitter_source = row.pop("twitter").strip()
    twitter = twitter_url(twitter_source)
    context.audit_data(
        row,
        ignore=[
            "orden",
            "picture",
            "description",
            "description_persona",
            "img",
            "funcion_estado_id",
            "funcion_estado",
            "institucion",
            "es_candidato",
        ],
    )

    position = h.make_position(
        context,
        name=position_name,
        country="ec",
        lang="spa",
        translate_name=False,
    )
    categorisation = categorise(context, position, default_is_pep=None)
    if not categorisation.is_pep:
        return

    person = context.make("Person")
    person.id = context.make_slug(source_id)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="spa")
    # Ecuador's public-service law allows qualifying foreign residents to hold many
    # of the mixed roles in this source, so country is accurate while citizenship
    # cannot be inferred (LOSEP Art. 5):
    # https://www.asambleanacional.gob.ec/sites/default/files/private/asambleanacional/filesasambleanacionalnameuid-20/transparencia-2015/literal-a/a2/Ley%20Org%C3%A1nica%20de%20Servicio%20P%C3%BAblico.pdf
    person.add("country", "ec")
    person.add("sourceUrl", f"https://www.radiografiapolitica.org/perfil/{source_id}")
    person.add("political", party)
    person.add("website", facebook)
    person.add(
        "website",
        twitter,
        original_value=twitter_source
        if twitter and twitter != twitter_source
        else None,
    )

    enrichment = enrichments.get(source_id)
    if enrichment is not None:
        h.apply_date(person, "birthDate", enrichment["birth_date"])
        person.add("gender", enrichment["gender"])

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(position)
    context.emit(person)


def crawl(context: Context) -> None:
    enrichment_payload = context.fetch_json(
        context.dataset.config["enrichment_url"], cache_days=1
    )
    assert isinstance(enrichment_payload, dict), type(enrichment_payload)
    enrichments = build_enrichment_index(context, enrichment_payload)

    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    for row in iter_current_rows(Path(path), dict(context.dataset.config)):
        crawl_row(context, row, enrichments)
