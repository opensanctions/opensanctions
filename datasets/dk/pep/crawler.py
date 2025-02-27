from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

RESOURCES = [
    ("dk", "dan", "dk.xlsx", "PEP_listen.xlsx"),
    ("fo", "fao", "fo.xlsx", "PEP_listen f"),
    ("gl", "kal", "gl.xlsx", "PEP_listen g"),
]


def make_position_name(category: str, job_title: str) -> str:
    if category == "Medlemmer af Folketinget":
        position_name = "Member of the Folketing"
    elif category == "Medlemmer af Europaparlamentet":
        position_name = "Member of the European Parliament"
    elif "Medlemmer af Lagtinget" in category:
        position_name = "Member of the Lagting"
    elif "Medlemmer" in category and "Inatsisartut" in category:
        position_name = "Member of the Inatsisartut"
    elif "partiernes styrelsesorganer" in category:
        position_name = "Member of leadership of " + (job_title or "a political party")
    else:
        position_name = job_title
    return position_name


def crawl_current_pep_item(context: Context, country: str, lang: str, input_dict: dict):
    first_name = input_dict.pop("first_name")
    last_name = input_dict.pop("last_name")

    entity = context.make("Person")
    entity.id = context.make_slug(first_name, last_name)
    h.apply_name(entity, first_name=first_name, last_name=last_name)

    birth_date = input_dict.pop("birth_date")
    h.apply_date(entity, "birthDate", birth_date.strip() if birth_date else None)
    category = input_dict.pop("category")
    job_title = input_dict.pop("job_title")
    position_name = make_position_name(category, job_title)
    assert position_name != "Socialdemokratiet"
    assert position_name is not None, entity.id

    position = h.make_position(context, position_name, country=country, lang=lang)
    categorisation = categorise(context, position, is_pep=True)

    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        start_date=input_dict.pop("listing_date").strip(),
        categorisation=categorisation,
    )

    if occupancy:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(input_dict, ignore=["new_on_list"])


def crawl_old_pep_item(context: Context, country: str, lang: str, input_dict: dict):
    last_name = input_dict.pop("last_name")
    first_name = input_dict.pop("first_name")

    entity = context.make("Person")
    entity.id = context.make_slug(first_name, last_name)
    h.apply_name(entity, first_name=first_name, last_name=last_name)

    h.apply_date(entity, "birthDate", input_dict.pop("birth_date"))

    position = h.make_position(
        context, input_dict.pop("job_title"), country=country, lang=lang
    )

    occupation = h.make_occupancy(
        context,
        entity,
        position,
        True,
        end_date=input_dict.pop("removal_date").strip(),
        categorisation=categorise(context, position, is_pep=True),
    )

    if occupation:
        context.emit(entity)
        context.emit(position)
        context.emit(occupation)

    context.audit_data(input_dict)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    for country, lang, name, file_pattern in RESOURCES:
        links = doc.xpath(
            f'//h2[strong[contains(text(), "PEP-liste ")]]/following-sibling::*//a[contains(@href, "{file_pattern}")]'
        )
        assert len(links) == 1, (file_pattern, links)
        path = context.fetch_resource(name, links[0].get("href"))
        context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

        wb = load_workbook(path, read_only=True)

        # Crawl old PEP list
        old_sheet = (
            "Tidligere PEP'ere"
            if "Tidligere PEP'ere" in wb.sheetnames
            else "Tidligere PEP'er"
        )
        for item in h.parse_xlsx_sheet(context, wb[old_sheet], header_lookup="columns"):
            crawl_old_pep_item(context, country, lang, item)

        new_sheet = (
            "Nuværende PEP'ere"
            if "Nuværende PEP'ere" in wb.sheetnames
            else "Nuværende PEP'er"
        )
        # Crawl current PEP list
        category = None
        for item in h.parse_xlsx_sheet(context, wb[new_sheet], 1, "columns"):
            if all(v is None for v in item.values()):
                continue
            # Meaning "No boards"
            if item["last_name"] == "Ingen styrelser etc.":
                continue
            if (
                item["category"]
                and sum(1 if v else 0 for v in list(item.values())) == 1
            ):
                category = item["category"]
                continue
            else:
                item["category"] = category
            assert item["category"] is not None, item
            crawl_current_pep_item(context, country, lang, item)

        assert len(wb.sheetnames) == 2, wb.sheetnames
