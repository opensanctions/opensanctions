from urllib.parse import urljoin
from typing import Dict
from zavod import Context
from zavod import helpers as h
from normality import collapse_spaces, stringify
from rigour.mime.types import XLS
import pandas as pd


DATE_FORMAT = ["%d-%b"]  # 'DD-MMM' format


def crawl_row(context: Context, row: Dict[str, str]):
    requester = row.pop("REQUESTER")
    requesting_country = row.pop("REQUESTING COUNTRY")
    date_listed = row.pop("DATE LISTED")
    date_listed_iso = h.parse_date(date_listed, DATE_FORMAT)

    if not requester or not requesting_country or not date_listed:
        context.log.warning(
            "Missing requester, requesting country, or date listed", row=row
        )
        return

    entity = context.make("Company")
    entity.id = context.make_id(requester, date_listed)
    entity.add("name", requester)
    entity.add("country", requesting_country)
    entity.add("notes", date_listed_iso)
    context.emit(entity, target=True)


def crawl_xls(context: Context, url: str):
    path = context.fetch_resource("source.xls", url)
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)
    try:
        xls = pd.read_excel(path, sheet_name=None)  # Load all sheets into a dictionary
    except Exception as e:
        context.log.error(f"Failed to open Excel file: {e}")
        return

    for sheet_name, sheet in xls.items():
        headers = [collapse_spaces(h) for h in sheet.columns]
        for _, row in sheet.iterrows():
            data = {headers[i]: stringify(row.iloc[i]) for i in range(len(headers))}
            crawl_row(context, data)


def fetch_excel_url(context: Context) -> str:
    params = {"_": context.data_time.date().isoformat()}
    doc = context.fetch_html(context.data_url, params=params)
    for link in doc.findall(".//a"):
        href = urljoin(context.data_url, link.get("href"))
        if href.endswith(".xls"):
            return href
    raise ValueError("Could not find XLS file on the website")


def crawl(context: Context):
    context.log.info("Fetching data from the source")
    url = fetch_excel_url(context)
    if url.endswith(".xls"):
        crawl_xls(context, url)
    else:
        raise ValueError("Unknown file type: %s" % url)
