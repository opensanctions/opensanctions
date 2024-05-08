from typing import Generator, cast, List
from zavod import Context, helpers as h
import openpyxl
from openpyxl import load_workbook
from pantomime.types import XLSX
from normality import stringify, slugify
from datetime import datetime

CZECH_MONTH_MAPPING = {
    "ledna": "January",
    "února": "February",
    "března": "March",
    "dubna": "April",
    "května": "May",
    "června": "June",
    "července": "July",
    "srpna": "August",
    "září": "September",
    "října": "October",
    "listopadu": "November",
    "prosince": "December",
}


def translate_date(date: str) -> str:
    """
    Translates Czech month names to English month names in a given date string.
    """
    for czech_month, eng_month in CZECH_MONTH_MAPPING.items():
        date = date.replace(czech_month, eng_month)
    return date


def parse_sheet(
    sheet: openpyxl.worksheet.worksheet.Worksheet,
) -> Generator[dict, None, None]:
    headers = None
    for row in sheet:
        cells = [c.value for c in row]
        if headers is None:
            headers = []
            for idx, cell in enumerate(cells):
                if cell is None:
                    cell = f"column_{idx}"
                headers.append(slugify(cell, "_").lower())
            continue

        record = {}
        for header, value in zip(headers, cells):
            if isinstance(value, datetime):
                value = value.date()
            record[header] = stringify(value)
        if len(record) == 0:
            continue
        yield record


def crawl_item(input_dict: dict, context: Context):
    # Jméno fyzické osoby -> First name of the natural person
    first_name = input_dict.pop("jmeno_fyzicke_osoby")

    # Příjmení fyzické osoby / Název právnické osoby / Označení nebo název entity -> Last name of the natural person / Name of the legal entity / Designation or name of the entity
    last_name = input_dict.pop(
        "prijmeni_fyzicke_osoby_nazev_pravnicke_osoby_oznaceni_nebo_nazev_entity"
    )

    # Datum narození fyzické osoby -> Date of birth of the natural person
    birth_date = input_dict.pop("datum_narozeni_fyzicke_osoby")

    # Státní příslušnost fyzické osoby / sídlo právnické osoby -> Nationality of the natural person / registered office of the legal entity
    res = context.lookup(
        "countries_names",
        input_dict.pop("statni_prislusnost_fyzicke_osoby_sidlo_pravnicke_osoby"),
    )
    if res:
        countries = cast("List[str]", res.names)
    else:
        context.log.warning("Country not identified", text=res)
        countries = None

    if first_name is None:
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(last_name)
        # There can be multiple names which are separated by /
        for name in last_name.split("/"):
            entity.add("name", name)

        entity.add("mainCountry", countries, lang="cz")

        # Další identifikační údaje -> Other identification data
        entity.add("notes", input_dict.pop("dalsi_identifikacni_udaje"), lang="cz")

    else:
        entity = context.make("Person")
        entity.id = context.make_slug(first_name, last_name)
        # There can be multiple names which are separated by /
        for name in last_name.split("/"):
            entity.add("lastName", name)
        for name in first_name.split("/"):
            entity.add("firstName", name)

        entity.add(
            "birthDate", h.parse_date(translate_date(birth_date), formats=["%d. %B %Y"])
        )

        entity.add("nationality", countries, lang="cz")

        # Další identifikační údaje -> Other identification data
        entity.add("notes", input_dict.pop("dalsi_identifikacni_udaje"), lang="cz")

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "startDate",
        h.parse_date(
            translate_date(input_dict.pop("datum_zapisu")), formats=["%d. %B %Y"]
        ),
    )

    # Popis postižitelného jednání -> Description of punishable conduct
    sanction.add("reason", input_dict.pop("popis_postizitelneho_jednani"), lang="cz")

    # Omezující opatření -> Restrictive measures
    sanction.add("provisions", input_dict.pop("omezujici_opatreni"), lang="cz")

    # Číslo usnesení vlády -> Government resolution number
    sanction.add("recordId", input_dict.pop("cislo_usneseni_vlady"), lang="cz")

    context.emit(entity, target=True)
    context.emit(sanction)

    # Ustanovení předpisu Evropské unie, jehož skutkovou podstatu subjekt jednáním naplnil -> Provision of the European Union regulation, the factual basis of which the subject fulfilled by action
    context.audit_data(
        input_dict,
        ignore=[
            "ustanoveni_predpisu_evropske_unie_jehoz_skutkovou_podstatu_subjekt_jednanim_naplnil"
        ],
    )


def crawl(context: Context):
    # First we find the link to the excel file

    response = context.fetch_html(context.data_url)

    response.make_links_absolute(context.data_url)

    # The link is the a tag with the title "Vnitrostátní sankční seznam" (National Sanctions List)
    # And the href contains ".xlsx"
    xpath = './/a[contains(@href, ".xlsx")][@title="Vnitrostátní sankční seznam"]'

    excel_link = response.xpath(xpath)[0].get("href")
    path = context.fetch_resource("list.xlsx", excel_link)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in parse_sheet(wb["List1"]):
        crawl_item(item, context)
