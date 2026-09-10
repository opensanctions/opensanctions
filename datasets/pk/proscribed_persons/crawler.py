from pathlib import Path
import shutil
import orjson
from rigour.mime.types import JSON
from stdnum.pk import cnic as cnic_validator

from zavod import Context
from zavod import helpers as h

# 4th Schedule under the Anti Terrorism Act, 1997
PROGRAM_KEY = "PK-ATA1997"
CNIC_LENGTH = 13

LOCAL_PATH = Path(__file__).parent


def is_placeholder_cnic(context: Context, cnic: str) -> bool:
    """Check the CNIC against the filler values listed in the dataset metadata."""
    return context.lookup("placeholder_cnic", cnic) is not None


def crawl_person(context: Context, row: dict[str, str]) -> None:
    person_name = row.pop("Name")
    father_name = row.pop("FatherName")
    cnic: str | None = row.pop("CNIC")
    province = row.pop("Province")
    district = row.pop("District")

    entity = context.make("Person")
    if cnic is None or is_placeholder_cnic(context, cnic):
        # Either no number at all, or a filler that identifies nobody. Neither may
        # become the entity ID, and neither may be published as an identifier.
        entity.id = context.make_slug(person_name, district, province)
        cnic = None
    elif cnic_validator.is_valid(cnic):
        cnic = cnic_validator.compact(cnic)
        entity.id = context.make_slug(cnic, prefix="pk-cnic")
    else:
        # The number is malformed, usually a wrong province or gender digit, but it
        # is still what the publisher holds for this person, so keep it searchable.
        # A fragment shorter than a full CNIC identifies nobody, so drop it.
        entity.id = context.make_slug(person_name, district, province)
        cnic = cnic_validator.compact(cnic)
        if len(cnic) != CNIC_LENGTH:
            context.log.warning("Discarding CNIC fragment", cnic=cnic, name=person_name)
            cnic = None
    entity.add("idNumber", cnic)
    # Everyone on the 4th Schedule is proscribed by a Pakistani authority, so the
    # country holds whether or not the CNIC is usable.
    entity.add("country", "pk")

    name_split = person_name.split("@")
    if len(name_split) > 1:
        person_name = name_split[0]
        entity.add("alias", name_split[1:])

    entity.add("name", person_name)
    entity.add("fatherName", father_name)
    entity.add("topics", "crime.terror")
    entity.add("topics", "wanted")
    entity.add("address", f"{district}, {province}")

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    source_path = LOCAL_PATH / "source.json"
    data = orjson.loads(source_path.read_bytes())
    # Export the source data as a resource by copying it from the dataset folder
    resource_path = context.get_resource_path("source.json")
    shutil.copy(source_path, resource_path)
    context.export_resource(resource_path, JSON, context.SOURCE_TITLE)

    for record in data:
        crawl_person(context, record)
