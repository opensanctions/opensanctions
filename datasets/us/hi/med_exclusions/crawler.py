import re
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from normality import squash_spaces

from zavod.extract import zyte_api

AKA_SPLIT = r"\baka\b|\ba\.k\.a\b|\bAKA\b|\bor\b"


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    if raw_first_name := row.pop("first_name"):
        entity = context.make("Person")

        raw_last_name = row.pop("last_name_or_business_name")
        raw_middle_initial = row.pop("middle_initial")
        dba = None
        if raw_last_name and " DBA " in raw_last_name:
            result = context.lookup("names", raw_last_name)
            if result is not None:
                raw_last_name, dba = result.values[0], result.values[1]
            else:
                context.log.warning("No lookups found for", raw_last_name)

        entity.id = context.make_id(
            raw_last_name, raw_middle_initial, raw_first_name, row.get("exclusion_date")
        )
        for first_name in re.split(AKA_SPLIT, raw_first_name):
            for last_name in re.split(AKA_SPLIT, raw_last_name or ""):
                for middle_initial in re.split(AKA_SPLIT, raw_middle_initial or ""):
                    h.apply_name(
                        entity,
                        first_name=first_name,
                        last_name=last_name,
                        middle_name=middle_initial,
                    )
                    entity.add("alias", dba)
    else:
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name_or_business_name"))
        entity.add("name", row.pop("last_name_or_business_name"))

    entity.add("sector", row.pop("last_known_program_or_provider_type"))
    medicaid_provider_id = row.pop("medicaid_provider_id")
    if medicaid_provider_id is not None and medicaid_provider_id != "NONE":
        entity.add("description", "Provider ID: " + medicaid_provider_id)
    else:
        row.pop("medicaid_provider_id")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("exclusion_date"))
    h.apply_date(sanction, "endDate", row.pop("reinstatement_date"))

    is_debarred = h.is_active(sanction)
    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_pdf_url(context: Context) -> str:
    download_xpath = ".//a[contains(text(), 'Med Prov Excl-Rein List')]"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=download_xpath,
        geolocation="us",
        absolute_links=True,
    )
    url = h.xpath_string(doc, download_xpath + "/@href")
    assert url is not None, "Could not find PDF URL"
    return url


def crawl(context: Context) -> None:
    _, _, _, path = zyte_api.fetch_resource(
        context, "source.pdf", crawl_pdf_url(context), PDF, geolocation="us"
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for item in h.parse_pdf_table(context, path, headers_per_page=True):
        for key, value in item.items():
            item[key] = squash_spaces(value) if value else None
        crawl_item(item, context)
