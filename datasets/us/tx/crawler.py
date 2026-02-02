from typing import Any, Dict
import xlrd
from xlrd.xldate import xldate_as_datetime

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from rigour.mime.types import XLS


def row_to_dict(row: Dict[str, Any], headers: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for norm, col in headers.items():
        value = row[col].value
        if isinstance(value, str):
            value = value.strip()
        if value is None or value == "":
            continue
        out[norm] = value
    return out


def crawl_item(drow: Dict[str, Any], context: Context, wb: xlrd.book.Book) -> None:
    for k, v in drow.items():
        if isinstance(v, str) and v.lower() in ["n/a"]:  # normalize NAs in values
            drow[k] = None

    last_name = drow.pop("LastName", None) or None
    company = drow.pop("CompanyName", None) or None
    npi = drow.pop("NPI", None) or None
    LicenseNumber = drow.pop("LicenseNumber", None) or None

    if not last_name and not company:
        return

    # convert excel dates to "%m/%d/%Y" datetime
    for key in ("StartDate", "AddDate"):
        val = drow.get(key)
        if isinstance(val, (int, float)):
            drow[key] = xldate_as_datetime(val, wb.datemode).strftime("%m/%d/%Y")

    if last_name:
        entity = context.make("Person")
        entity.id = context.make_id(last_name, LicenseNumber)
        entity.add("lastName", last_name)
        entity.add("firstName", drow.pop("FirstName", None) or None)
        entity.add("middleName", drow.pop("MidInitial", None) or None)
        entity.add("profession", drow.pop("Occupation", None) or None)

    else:
        entity = context.make("Company")
        entity.id = context.make_id(
            company
        )  # note that npi/licenseNumber isnt listed for all companies though so not including in ID
        entity.add("name", company)
        entity.add("description", drow.pop("Occupation", None) or None)

    entity.add("licenseNumber", LicenseNumber)  # or idNumber?
    entity.add("npiCode", npi)
    entity.add("country", "us")
    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", drow.pop("StartDate", None))
    h.apply_date(sanction, "listingDate", drow.pop("AddDate", None) or None)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        drow,
        ignore=["ReinstatedDate", "WebComments", "Waiver", "EligibleToReapplyDate"],
    )


def crawl(context: Context) -> None:
    """
    Website is geofenced (U.S.). Connection to a US server is required.
    """
    # fetch the XLS file via ASP.NET postback like in datasets/sk/public_officials
    # doc = context.fetch_html(context.data_url)
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

    path = context.fetch_resource(
        "source.xls",
        context.data_url,
        method="POST",
        data=form_params,
    )  # really need to remember to delete files in datasets/us_tx_oig_exclusions in case of a bad first download...

    context.export_resource(path, XLS, title=context.SOURCE_TITLE)

    wb = xlrd.open_workbook(path)  # old .xls version not supported by openpyxl
    sh = wb.sheet_by_index(0)

    if wb.nsheets != 1:
        context.log.warning(f"Expected one sheet in workbook, got {wb.nsheets}")

    headers = {}
    for col_idx, cell in enumerate(sh.row(0)):
        val = str(cell.value).strip()
        headers[val] = col_idx
        # headers[context.lookup_value("headers", val, val)] = col_idx # yml lookup for headers like in id/dttot

    # row_id = 0
    assert wb.datemode == 0  # to parse old excel dates
    for rx in range(1, sh.nrows):
        # row_id += 1
        drow = row_to_dict(sh.row(rx), headers)
        # if row_id == 1577:
        #     breakpoint()
        crawl_item(drow, context, wb)
