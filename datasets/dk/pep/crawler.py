from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

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


def crawl_current_pep_item(
    context: Context, *, country: str, lang: str, row: dict[str, str | None]
) -> None:
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")

    entity = context.make("Person")
    entity.id = context.make_slug(first_name, last_name)
    h.apply_name(entity, first_name=first_name, last_name=last_name)

    birth_date = row.pop("birth_date")
    h.apply_date(entity, "birthDate", birth_date.strip() if birth_date else None)
    category = row.pop("category")
    job_title = row.pop("job_title")
    assert job_title is not None, row
    assert category is not None, row
    position_name = make_position_name(category, job_title)
    assert position_name != "Socialdemokratiet"
    assert position_name is not None, entity.id

    position = h.make_position(context, position_name, country=country, lang=lang)
    categorisation = categorise(context, position, is_pep=True)

    listing_date = row.pop("listing_date")
    assert listing_date is not None, row
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        True,
        start_date=listing_date.strip(),
        categorisation=categorisation,
    )

    if occupancy:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(row, ignore=["new_on_list"])


def crawl_old_pep_item(
    context: Context, *, country: str, lang: str, row: dict[str, str | None]
) -> None:
    last_name = row.pop("last_name")
    first_name = row.pop("first_name")

    entity = context.make("Person")
    entity.id = context.make_slug(first_name, last_name)
    h.apply_name(entity, first_name=first_name, last_name=last_name)

    h.apply_date(entity, "birthDate", row.pop("birth_date"))

    job_title = row.pop("job_title")
    assert job_title is not None, row
    position = h.make_position(context, job_title, country=country, lang=lang)
    removal_date = row.pop("removal_date")
    assert removal_date is not None, row
    occupation = h.make_occupancy(
        context,
        entity,
        position,
        True,
        end_date=removal_date.strip(),
        categorisation=categorise(context, position, is_pep=True),
    )

    if occupation:
        context.emit(entity)
        context.emit(position)
        context.emit(occupation)

    context.audit_data(row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)

    for country, lang, name, file_pattern in RESOURCES:
        link = h.xpath_string(
            doc,
            f'//h2[strong[contains(text(), "PEP-liste ")]]/following-sibling::*//a[contains(@href, "{file_pattern}")]/@href',
        )
        path = context.fetch_resource(name, link)
        context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

        wb = load_workbook(path, read_only=True)

        # Crawl old PEP list
        old_sheet = (
            "Tidligere PEP'ere"
            if "Tidligere PEP'ere" in wb.sheetnames
            else "Tidligere PEP'er"
        )
        for row in h.parse_xlsx_sheet(
            context, wb[old_sheet], header_lookup=context.get_lookup("columns")
        ):
            crawl_old_pep_item(context, country=country, lang=lang, row=row)

        new_sheet = (
            "Nuværende PEP'ere"
            if "Nuværende PEP'ere" in wb.sheetnames
            else "Nuværende PEP'er"
        )
        # Crawl current PEP list
        category = None
        for row in h.parse_xlsx_sheet(
            context, wb[new_sheet], 1, header_lookup=context.get_lookup("columns")
        ):
            if not any([row["category"], row["first_name"], row["last_name"]]):
                continue
            # Meaning "No boards"
            if row["last_name"] == "Ingen styrelser etc.":
                continue

            if row["category"] and sum(1 if v else 0 for v in list(row.values())) == 1:
                category = row["category"]
                continue
            else:
                row["category"] = category
            assert row["category"] is not None, row

            crawl_current_pep_item(context, country=country, lang=lang, row=row)

        assert len(wb.sheetnames) == 2, wb.sheetnames
