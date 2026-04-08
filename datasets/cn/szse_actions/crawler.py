from openpyxl import load_workbook
from typing import Dict, Any

from zavod import Context, helpers as h
from rigour.mime.types import XLSX


def crawl_actions_item(context: Context, row: Dict[str, Any]) -> None:
    id = row.pop("company_code")
    name = row.pop("company_name")

    # skip empty company names (44 cases)
    if name is None:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(id, name)
    entity.add("name", name)
    entity.add("country", "cn")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("date_of_action"))
    sanction.add(
        "sourceUrl",
        "https://reportdocs.static.szse.cn/UpFiles/zqjghj/" + row.pop("letter_pdf"),
    )
    sanction.add("summary", row.pop("decision_summary"))
    sanction.add("provisions", row.pop("action_type"))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource(
        "source.xlsx", context.data_url, headers={"Referer": context.data_url}
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=False)
    if len(wb.sheetnames) != 1:
        raise ValueError(f"Expected 1 sheet, got {len(wb.sheetnames)} in source.xlsx")
    for row in h.parse_xlsx_sheet(
        context, wb[wb.sheetnames[0]], header_lookup=context.get_lookup("columns")
    ):
        crawl_actions_item(context, row)
