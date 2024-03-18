from typing import IO
from pathlib import Path
from zipfile import ZipFile
import xml.etree.ElementTree as ET

from zavod import Context
from zavod import helpers as h

NS = "{urn:iso:std:iso:20022:tech:xsd:auth.017.001.02}"


def parse_element(context: Context, elem: ET.Element) -> None:
    attr = elem.find(f"./{NS}FinInstrmGnlAttrbts")
    if attr is None:
        return
    isin = attr.findtext(f"./{NS}Id")
    if isin is None:
        context.log.warn("No ISIN", elem=elem)
        return
    security = h.make_security(context, isin)
    security.add("name", attr.findtext(f"./{NS}FullNm"))
    security.add("alias", attr.findtext(f"./{NS}ShrtNm"))
    security.add("classification", attr.findtext(f"./{NS}ClssfctnTp"))
    security.add("currency", attr.findtext(f"./{NS}NtnlCcy"))
    trading = elem.find(f"./{NS}TradgVnRltdAttrbts")
    if trading is not None:
        security.add("createdAt", trading.findtext(f"./{NS}AdmssnApprvlDtByIssr"))

    lei = elem.findtext(f"./{NS}Issr")
    if lei is not None:
        lei_id = f"lei-{lei}"
        issuer = context.make("Organization")
        issuer.id = lei_id
        issuer.add("leiCode", lei)
        context.emit(issuer)
        security.add("issuer", lei_id)

    context.emit(security)


def parse_xml_doc(context: Context, file: IO[bytes]) -> None:
    for (event, elem) in ET.iterparse(file, events=("end",)):
        if event == "end" and elem.tag == f"{NS}RefData":
            parse_element(context, elem)
            elem.clear()


def parse_xml_file(context: Context, path: Path) -> None:
    with ZipFile(path) as archive:
        for name in archive.namelist():
            if name.endswith(".xml"):
                with archive.open(name) as fh:
                    parse_xml_doc(context, fh)


def get_full_dumps_index(context: Context):
    query = {
        "core": "esma_registers_firds_files",
        "pagingSize": "100",
        "start": 0,
        "keyword": "",
        "sortField": "publication_date desc",
        "criteria": [
            {
                "name": "file_type",
                "value": "file_type:FULINS",
                "type": "custom1",
                "isParent": True,
            },
        ],
        "wt": "json",
    }
    resp = context.http.post(context.data_url, json=query)
    resp_data = resp.json()
    latest = None
    for result in resp_data["response"]["docs"]:
        if latest is not None and latest != result["publication_date"]:
            break
        latest = result["publication_date"]
        yield result["file_name"], result["download_link"]


def crawl(context: Context) -> None:
    for file_name, url in get_full_dumps_index(context):
        context.log.info("Fetching %s" % file_name, url=url)
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
