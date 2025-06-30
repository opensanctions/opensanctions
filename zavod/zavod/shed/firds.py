from collections import defaultdict
import os
from typing import List, Tuple
from lxml import etree
from pathlib import Path
from zipfile import ZipFile
from rigour.ids import ISIN
from tempfile import TemporaryDirectory
import re

from zavod import Context
from zavod import helpers as h

REGEX_DATE = re.compile(r"_(20\d{6})_")
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


def latest_full_set(
    context: Context, dump_urls: List[Tuple[str, str]]
) -> List[Tuple[str, str]]:
    """Given a list of (file_name, url) tuples, return the items for the latest date
    occurring in the list."""
    date_sets = defaultdict(list)
    for file_name, url in dump_urls:
        match = REGEX_DATE.search(url)
        if not match:
            context.log.warning(f"URL {url} does not match expected date format.")
            continue
        date_str = match.group(1)
        date_sets[date_str].append((file_name, url))
    latest = max(date_sets.keys())
    return date_sets[latest]
