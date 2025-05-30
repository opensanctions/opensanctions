import os
from lxml import etree
from pathlib import Path
from zipfile import ZipFile
from rigour.ids import ISIN
from tempfile import TemporaryDirectory

from zavod import Context
from zavod import helpers as h

NS = "{urn:iso:std:iso:20022:tech:xsd:auth.017.001.02}"


def parse_element(context: Context, elem: etree._Element) -> None:
    attr = elem.find(f"./{NS}FinInstrmGnlAttrbts")
    if attr is None:
        return
    isin = attr.findtext(f"./{NS}Id")
    if isin is None:
        context.log.warn("No ISIN", elem=elem)
        return
    if not ISIN.is_valid(isin):
        # Skip OTC derivatives and other special case securities
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


def parse_xml_doc(context: Context, path: str) -> None:
    for _, elem in etree.iterparse(path, events=("end",), tag=f"{NS}RefData"):
        parse_element(context, elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def parse_xml_file(context: Context, path: Path) -> None:
    with TemporaryDirectory() as tmpdir:
        with ZipFile(path) as archive:
            for name in archive.namelist():
                if not name.endswith(".xml"):
                    continue
                tmpfile = archive.extract(name, path=tmpdir)
                context.log.info("Reading XML file", path=tmpfile)
                parse_xml_doc(context, tmpfile)
                os.unlink(tmpfile)


def get_full_dumps_index(context: Context):
    page = 0
    size = 100
    total = None
    while True:
        url = f"{context.data_url}?q=file_type:FULINS&from={page * size}&size={size}&pretty=true"
        data = context.fetch_json(url)
        if total is None:
            total = data["hits"]["total"]
            context.log.info(f"Total FIRDS files to fetch: {total}")
        hits = data["hits"]["hits"]
        if not hits:
            break
        for hit in hits:
            src = hit["_source"]
            yield src["file_name"], src["download_link"]
        page += 1
        if page * size >= total:
            break


def crawl(context: Context) -> None:
    for file_name, url in get_full_dumps_index(context):
        context.log.info("Fetching %s" % file_name, url=url)
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
