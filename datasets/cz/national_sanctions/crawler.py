import csv
import io
import re
from typing import Dict, List
from rigour.mime.types import CSV
from normality import slugify
from rigour.names import pick_name

from zavod import Context, helpers as h


# The entire string is mostly question marks, with perhaps a space or hyphen in there.
REGEX_ENCODING_MISHAP = re.compile(r"^[\? -]+$")
# detect anything more complex than word/word/word (and handle question mark woopsie)
REGEX_LAST_NAME = re.compile(r"^[\w\?]+( ?/ ?[\w\?]+)*$")


def clean_names(names: List[str]) -> List[str]:
    return [name for name in names if not REGEX_ENCODING_MISHAP.match(name)]


def crawl_item(row: Dict[str, str], context: Context):
    # Jméno fyzické osoby
    # -> First name of the natural person
    first_name_field = row.pop("jmeno_fyzicke_osoby", "").strip('"').strip()
    # Cleaning here rather than via type.string lookup because we assemble full
    # names from these.
    first_names = clean_names(first_name_field.split("/"))

    # Příjmení fyzické osoby / Název právnické osoby / Označení nebo název entity
    # -> Last name of the natural person / Name of the legal entity / Designation or name of the entity
    name_field = row.pop(
        "prijmeni_fyzicke_osoby_nazev_pravnicke_osoby_oznaceni_nebo_nazev_entity"
    )
    name_field = str(name_field)

    # Datum narození fyzické osoby
    # -> Date of birth of the natural person
    birth_date = row.pop("datum_narozeni_fyzicke_osoby")

    # Státní příslušnost fyzické osoby / sídlo právnické osoby
    # -> Nationality of the natural person / registered office of the legal entity
    countries = row.pop("statni_prislusnost_fyzicke_osoby_sidlo_pravnicke_osoby")

    sanction_props = dict()
    if REGEX_LAST_NAME.match(name_field):
        names = clean_names(name_field.split("/"))
    else:
        res = context.lookup("name_notes", name_field)
        if res:
            names = res.names
            sanction_props = res.sanction_props
        else:
            names = clean_names(name_field.split("/"))
            context.log.warning("Name field needs manual cleaning", name=name_field)

    if len(first_name_field) == 0:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name_field, first_name_field, countries)
        # There can be multiple names which are separated by /
        entity.add("name", names)
        entity.add("country", countries.split(", "), lang="ces")
    else:
        entity = context.make("Person")
        entity.id = context.make_id(name_field, first_name_field, countries)
        # There can be multiple names which are separated by /
        entity.add("lastName", names)
        entity.add("firstName", first_names)
        h.apply_name(
            entity,
            first_name=pick_name(first_names),
            last_name=pick_name(names),
        )
        h.apply_date(entity, "birthDate", birth_date)
        entity.add("nationality", countries.split(", "), lang="ces")

    # Další identifikační údaje
    # -> Other identification data
    entity.add("notes", row.pop("dalsi_identifikacni_udaje"), lang="ces")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("datum_zapisu"))
    for prop, value in sanction_props.items():
        if "date" in prop.lower():
            if isinstance(value, str):
                h.apply_date(sanction, prop, value)
            elif isinstance(value, list):
                h.apply_dates(sanction, prop, value)
            else:
                raise Exception(
                    f"Unexpected value type {type(value)} for {prop}: {value}"
                )
        else:
            sanction.add(prop, value, lang="ces")

    if h.is_active(sanction):
        entity.add("topics", "sanction")

    # Popis postižitelného jednání -> Description of punishable conduct
    sanction.add("reason", row.pop("popis_postizitelneho_jednani"), lang="ces")

    # Omezující opatření -> Restrictive measures
    sanction.add("provisions", row.pop("omezujici_opatreni"), lang="ces")

    # Číslo usnesení vlády
    # -> Government resolution number
    sanction.add("recordId", row.pop("cislo_usneseni_vlady"), lang="ces")

    # Ustanovení předpisu Evropské unie, jehož skutkovou podstatu subjekt jednáním naplnil
    # -> Provision of the European Union regulation, the factual basis of which the subject fulfilled by action
    provision = row.pop(
        "ustanoveni_predpisu_evropske_unie_jehoz_skutkovou_podstatu_subjekt_jednanim_naplnil"
    )
    sanction.add("program", provision, lang="ces")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_csv_url(context: Context):
    doc = context.fetch_html(context.data_url)
    # (etree.tostring(doc))
    doc.make_links_absolute(context.data_url)
    anchor = doc.xpath('//a[@title="Vnitrostátní sankční seznam"]')
    if len(anchor) == 0:
        raise Exception("No sanctions file link found")
    return anchor[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    csv_url = crawl_csv_url(context)
    path = context.fetch_resource("source.csv", csv_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        fdata = fh.read()
        fdata = re.sub(
            r"\"+(?=[^\",])", '"', fdata
        )  # Singlify all double quotes in front of a non-comma/quote character
        fdata = re.sub(
            r"(?<=[^\",])\"+", '"', fdata
        )  # Singlify all double quotes after a non-comma/quote character
        fdata = re.sub(
            r"(?<=,)\"+(?=,)", '""', fdata
        )  # Switch all quotes between commas with two quotes
        strio = io.StringIO(fdata)
        for record in csv.DictReader(strio):
            row_ = {slugify(k, "_"): str(v) for k, v in record.items() if v is not None}
            row = {k: v for k, v in row_.items() if k is not None}
            crawl_item(row, context)
