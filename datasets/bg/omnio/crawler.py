import csv
from typing import Dict

from rigour.mime.types import CSV
from zavod.shed.names.split import LLM_MODEL_VERSION
from zavod.stateful.review import TextSourceValue, review_extraction

from zavod import Context
from zavod import helpers as h

TYPES = {
    "Person": "Person",
    "Organization": "Organization",
    "": "LegalEntity",
}
ALIAS_PROPS = [
    ("full_name", "alias"),
    ("alias", "alias"),
    ("weak_alias", "weakAlias"),
    ("previous_name", "previousName"),
]


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make(TYPES[row.pop("Entity_Type")])
    row.pop("Entity_Type_BG")
    entity.id = context.make_id(
        row.get("Label"),
        row.get("First_Name"),
        row.get("First_Name_BG"),
        row.get("Second_Name"),
        row.get("Second_Name_BG"),
        row.get("Family_Name"),
        row.get("Family_Name_BG"),
        row.get("DOB"),
        row.get("Place_of_birth"),
        row.get("Place_of_birth_BG"),
    )
    if entity.id is None:
        context.log.warning("Skip row", row=row)
        return

    h.apply_name(
        entity,
        first_name=row.pop("First_Name"),
        second_name=row.pop("Second_Name"),
        last_name=row.pop("Family_Name"),
        quiet=True,
    )

    h.apply_name(
        entity,
        first_name=row.pop("First_Name_BG"),
        second_name=row.pop("Second_Name_BG"),
        last_name=row.pop("Family_Name_BG"),
        alias=True,
        quiet=True,
        lang="bul",
    )

    entity.add("topics", "sanction")
    # entity.add("name", row.pop("Label"), lang="bul")

    aliases = row.pop("Aliases")
    if h.needs_splitting(entity.schema, aliases):
        names = h.split_names(context, aliases)
        source_value = TextSourceValue(
            key_parts=aliases, label="translated aliases", text=aliases
        )
        review = review_extraction(
            context,
            source_value=source_value,
            original_extraction=names,
            origin=LLM_MODEL_VERSION,
        )

        if review.accepted:
            for field_name, prop in ALIAS_PROPS:
                for name in getattr(review.extracted_data, field_name):
                    entity.add(
                        prop,
                        name,
                        lang="bul",
                        origin=review.origin,
                        original_value=review.source_value,
                    )
        else:
            entity.add("alias", aliases, lang="bul")
    else:
        entity.add("alias", aliases, lang="bul")

    entity.add("alias", row.pop("Aliases_BG", "").split(";"), lang="bul")
    entity.add("country", row.pop("Countries_of_Residence", "").split(";"))
    entity.add(
        "country", row.pop("Countries_of_Residence_BG", "").split(";"), lang="bul"
    )
    cit_prop = "citizenship" if entity.schema.is_a("Person") else "jurisdiction"
    entity.add(cit_prop, row.pop("Citizenships", "").split(";"), lang="eng")
    entity.add(cit_prop, row.pop("Citizenships_BG", "").split(";"), lang="bul")
    entity.add("birthPlace", row.pop("Place_of_birth"), quiet=True)
    entity.add("birthPlace", row.pop("Place_of_birth_BG"), quiet=True, lang="bul")
    for part in h.multi_split([row.pop("DOB"), row.pop("DOB_BG")], [";", "/"]):
        if entity.schema.is_a("Person"):
            h.apply_date(entity, "birthDate", part)
    entity.add("passportNumber", row.pop("Passport_No"), quiet=True)
    entity.add("passportNumber", row.pop("Passport_No_BG"), quiet=True, lang="bul")
    entity.add("idNumber", row.pop("National_ID"), quiet=True)
    entity.add("idNumber", row.pop("National_ID_BG"), quiet=True, lang="bul")
    entity.add("taxNumber", row.pop("Italian_Fiscal_Code", None), quiet=True)
    entity.add("taxNumber", row.pop("Italian_Fiscal_Code_BG"), quiet=True, lang="bul")
    entity.add("position", row.pop("Position"), quiet=True)
    entity.add("position", row.pop("Position_BG"), quiet=True, lang="bul")
    entity.add("sourceUrl", row.pop("Source_URL"))
    entity.add("sourceUrl", row.pop("Source_URL_BG"), lang="bul")
    entity.add("notes", row.pop("Notes"))
    entity.add("notes", row.pop("Notes_BG"), lang="bul")
    entity.add("address", row.pop("Addresses").split("; "))
    entity.add("address", row.pop("Addresses_BG").split("; "), lang="bul")
    # context.inspect(row)
    context.emit(entity)
    context.audit_data(
        row,
        ignore=[
            "",
            "Seq",
            "Label",
            "NR_BG",
            "Number",
            "Reason",
            "Record source/date",
            "Connections",
            "Connections_BG",
        ],
    )


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
