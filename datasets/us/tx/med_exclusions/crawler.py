from typing import Any, Dict
from urllib.parse import urlencode

import xlrd

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from rigour.mime.types import XLS
from rigour.names.split_phrases import contains_split_phrase


def crawl_row(drow: Dict[str, Any], context: Context, wb: xlrd.book.Book) -> None:
    for k, v in drow.items():
        if isinstance(v, str) and v.lower() in ["n/a"]:  # normalize NAs in values
            drow[k] = None

    last_name = drow.pop("lastname")
    first_name = drow.pop("firstname")
    middle_name = drow.pop("midinitial")
    company_name = drow.pop("companyname")
    npi = drow.pop("npi")
    license_number = drow.pop("licensenumber")
    occupation = drow.pop("occupation")

    if not last_name and not company_name:
        return

    if last_name:
        entity = context.make("Person")
        entity.id = context.make_id(first_name, middle_name, last_name, license_number)
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=last_name,
            middle_name=middle_name,
        )
        entity.add("position", occupation)

    else:
        entity = context.make("Company")
        entity.id = context.make_id(company_name, npi)
        entity.add("description", occupation)

        if contains_split_phrase(company_name):
            res = context.lookup("names", company_name, warn_unmatched=True)
            if res is not None:
                entity.add("name", res.names[0], original_value=company_name)
                entity.add("alias", res.names[1:], original_value=company_name)
            else:
                entity.add("name", company_name)
        else:
            entity.add("name", company_name)

    entity.add("idNumber", license_number)
    entity.add("npiCode", npi)
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add("description", drow.pop("webcomments"))

    h.apply_date(sanction, "startDate", drow.pop("startdate"))
    h.apply_date(sanction, "listingDate", drow.pop("adddate"))
    h.apply_date(sanction, "endDate", drow.pop("reinstateddate"))
    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        drow,
        ignore=["waiver", "eligibletoreapplydate"],
    )


def crawl(context: Context) -> None:
    """
    Website is geofenced (U.S.). Connection from a US server is required.
    """
    # fetch the XLS file via ASP.NET postback like in datasets/sk/public_officials
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=".//a[@id='dnn_ctr384_DownloadExclusionsFile_lb_DLoad_ExcFile_XLS']",
        geolocation="us",
    )
    viewstate = h.xpath_strings(
        doc, '//input[@name="__VIEWSTATE"]/@value', expect_exactly=1
    )[0]
    eventvalidation = h.xpath_strings(
        doc, '//input[@name="__EVENTVALIDATION"]/@value', expect_exactly=1
    )[0]

    form_params = {
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
        "__EVENTTARGET": "dnn$ctr384$DownloadExclusionsFile$lb_DLoad_ExcFile_XLS",
    }

    _, _, _, path = zyte_api.fetch_resource(
        context,
        "source.xls",
        context.data_url,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        body=urlencode(form_params).encode("utf-8"),
    )

    context.export_resource(path, XLS, title=context.SOURCE_TITLE)

    wb = xlrd.open_workbook(str(path))  # old .xls version not supported by openpyxl
    assert len(wb.sheets()) == 1
    sh = wb.sheet_by_name("EXCEL_Destination")
    for row in h.parse_xls_sheet(context, sh):
        crawl_row(row, context, wb)
