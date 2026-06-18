import re
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

REGEX_AKA = re.compile(r"^a\.k\.a\.? ", re.IGNORECASE)


def crawl_item(row: dict[str, str | None], context: Context) -> tuple[Entity, Entity]:
    if ", " not in (row.get("terminated_excluded_provider_s") or ""):
        entity = context.make("Company")
        entity.id = context.make_id(row.get("terminated_excluded_provider_s"))
        entity.add("name", row.pop("terminated_excluded_provider_s"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(row.get("terminated_excluded_provider_s"))
        name_raw = row.pop("terminated_excluded_provider_s")
        assert name_raw is not None
        last_name, first_name = name_raw.split(", ")
        h.apply_name(entity, first_name=first_name, last_name=last_name)

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("healthcare_profession"))
    entity.add("country", "us")
    entity.add("npiCode", row.pop("npi"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))

    context.audit_data(row, ignore=["column_4"])
    return entity, sanction


def crawl_excel_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    url = h.xpath_string(
        doc,
        ".//a[text()='Download Excluded or Terminated Provider list in Excel']/@href",
    )
    assert url is not None, "Could not find Excel file URL"
    return url


def crawl(context: Context) -> None:
    excel_url = crawl_excel_url(context)

    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    entity = None
    sanction = None
    current_alias = []
    assert wb.active is not None
    for item in h.parse_xlsx_sheet(context, wb.active):
        name = item.get("terminated_excluded_provider_s")
        if name and REGEX_AKA.match(name):
            current_alias.append(REGEX_AKA.sub("", name))
        else:
            # Move on to the next entity
            if entity is not None:
                entity.add("name", current_alias)
                context.emit(entity)
                assert sanction is not None
                context.emit(sanction)
            current_alias = []
            entity, sanction = crawl_item(item, context)
    # Emit the last entity
    assert entity is not None
    assert sanction is not None
    context.emit(entity)
    context.emit(sanction)
