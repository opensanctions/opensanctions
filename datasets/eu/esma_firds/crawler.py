from lxml import etree
from pathlib import Path
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from zavod.helpers.xml import ElementOrTree


def parse_xml_doc(context: Context, file: ElementOrTree) -> None:
    doc = file.find('.//Document')
    assert doc is not None, file
    for ref in doc.findall('./FinInstrmRptgRefDataRpt/RefData'):
        attr = ref.find('./FinInstrmGnlAttrbts')
        if attr is None:
            continue
        isin = attr.findtext('./Id')
        if isin is None:
            context.log.warn("No ISIN", ref=ref)
            continue
        security = context.make("Security")
        security.id = f"isin-{isin}"
        security.add("name", attr.findtext('./FullNm'))
        security.add("alias", attr.findtext('./ShrtNm'))
        security.add("classification", attr.findtext('./ClssfctnTp'))
        security.add("currency", attr.findtext('./NtnlCcy'))
        security.add("createdAt", ref.findtext('./TradgVnRltdAttrbts/AdmssnApprvlDtByIssr'))

        lei = ref.findtext('./Issr')
        if lei is not None:
            lei_id = f"lei-{lei}"
            issuer = context.make("Organization")
            issuer.id = lei_id
            issuer.add("leiCode", lei)
            context.emit(issuer)
            security.add("issuer", lei_id)
        
        context.emit(security)


def parse_xml_file(context: Context, path: Path) -> None:
    with ZipFile(path) as archive:
        for name in archive.namelist():
            if name.endswith(".xml"):
                with archive.open(name) as fh:
                    doc = h.remove_namespace(etree.parse(fh))
                    parse_xml_doc(context, doc)


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
    for result in resp_data['response']['docs']:
        if latest is not None and latest != result['publication_date']:
            break
        latest = result['publication_date']
        yield result['file_name'], result['download_link']
    


def crawl(context: Context) -> None:
    for file_name, url in get_full_dumps_index(context):
        context.log.info("Fetching %s" % file_name, url=url)
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
