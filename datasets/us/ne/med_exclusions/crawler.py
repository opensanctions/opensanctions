from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource

PAGE_SETTINGS = {"join_y_tolerance": 2}


def crawl_item(row: Dict[str, str], context: Context):
    first_name = row.pop("provider_first_nam")
    middle_name = row.pop("provider_middle_initia")
    last_name = row.pop("provider_last_name")
    npi = row.pop("npi")
    if organization_name := row.pop("organization_name"):
        entity = context.make("Company")
        entity.id = context.make_id(organization_name, npi)
        entity.add("name", organization_name)
        # Either empty or a copy
        assert last_name in organization_name, row
    else:
        entity = context.make("Person")
        entity.id = context.make_id(
            first_name,
            middle_name,
            last_name,
            npi,
        )
        h.apply_name(
            entity,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
        )
        assert organization_name == "", row

    entity.add("npiCode", npi)
    entity.add("sector", row.pop("eprovider_type_code"))
    entity.add("topics", "debarment")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop("reason_for_action_code"))
    h.apply_date(sanction, "startDate", row.pop("effective_dat"))
    sanction.add("provisions", row.pop("sanction_code"))
    sanction.add("duration", row.pop("sanction_type_code"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context, "source.pdf", context.data_url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(
        context,
        path,
        headers_per_page=True,
        page_settings=lambda page: (page, PAGE_SETTINGS),
    ):
        crawl_item(item, context)
