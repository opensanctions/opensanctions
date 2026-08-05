from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.extract.zyte_api import fetch_resource

PAGE_SETTINGS = {"join_y_tolerance": 2}


def key_from_prefix(row: dict[str, str | None], prefix: str) -> str:
    key = [k for k in row.keys() if k.startswith(prefix)]
    assert len(key) == 1, ("Cannot find key.", key, row)
    return key[0]


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    name = row.pop(key_from_prefix(row, "provider_name"))
    listing_date = row.pop(key_from_prefix(row, "date_added_to_nmep"))
    npi = row.pop("provider_npi")

    if organization_name := row.pop("organization_name"):
        entity = context.make("Company")
        entity.id = context.make_id(organization_name, npi)
        entity.add("name", organization_name)
    else:
        entity = context.make("Person")
        entity.id = context.make_id(name, npi)
        entity.add("name", name)
        assert organization_name in ("", None), (
            "Cannot have both person and organization name."
        )

    entity.add("npiCode", npi)
    entity.add("sector", row.pop("provider_type_code"))
    entity.add("topics", "debarment")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop("reason_for_action_code"))
    h.apply_date(sanction, "startDate", row.pop(key_from_prefix(row, "effective_date")))
    h.apply_date(sanction, "listingDate", listing_date)
    sanction.add("provisions", row.pop("sanction_code"))
    sanction.add("duration", row.pop("sanction_type_code"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context,
        "source.pdf",
        context.data_url,
        expected_media_type=PDF,
        geolocation="US",
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(
        context,
        path,
        page_settings=lambda page: (page, PAGE_SETTINGS),
    ):
        crawl_item(item, context)
