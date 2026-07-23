from collections import defaultdict
import csv
import os
from typing import Iterable, List, Mapping, Tuple
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
# The ISO 10383 registry of Market Identifier Codes, which includes the LEI of
# the operator of each market segment.
MIC_REGISTRY_URL = (
    "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"
)


def load_lei_mics(context: Context) -> dict[str, set[str]]:
    """Map market operator LEIs to the MICs they operate, per ISO 10383."""
    path = context.fetch_resource("ISO10383_MIC.csv", MIC_REGISTRY_URL)
    lei_mics: dict[str, set[str]] = defaultdict(set)
    with open(path, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            lei = row["LEI"]
            if lei != "":
                lei_mics[lei].add(row["MIC"])
    return dict(lei_mics)


def parse_element(
    context: Context,
    file_name: str,
    elem: etree._Element,
    lei_mics: Mapping[str, set[str]],
) -> None:
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
        # The Issr field holds "the issuer or the operator of the trading
        # venue". When its LEI belongs to the operator of the record's own
        # relevant trading venue, it is likely not the actual issuer.
        venue_mic = elem.findtext(f"./{NS}TechAttrbts/{NS}RlvntTradgVn")
        if venue_mic is not None and venue_mic in lei_mics.get(lei, set()):
            context.log.info(
                "Issr LEI is the operator of the relevant trading venue",
                isin=isin,
                lei=lei,
                mic=venue_mic,
            )
        lei_id = f"lei-{lei}"
        issuer = context.make("Organization")
        issuer.id = lei_id
        issuer.add("leiCode", lei)
        context.emit(issuer, origin=file_name)
        security.add("issuer", lei_id)

    context.emit(security, origin=file_name)


def parse_xml_doc(
    context: Context, file_name: str, path: str, lei_mics: Mapping[str, set[str]]
) -> None:
    for _, elem in etree.iterparse(path, events=("end",), tag=f"{NS}RefData"):
        parse_element(context, file_name, elem, lei_mics)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def parse_xml_file(context: Context, path: Path) -> None:
    lei_mics = load_lei_mics(context)
    with TemporaryDirectory() as tmpdir:
        with ZipFile(path) as archive:
            for name in archive.namelist():
                if not name.endswith(".xml"):
                    continue
                tmpfile = archive.extract(name, path=tmpdir)
                context.log.info("Reading XML file", path=tmpfile)
                parse_xml_doc(context, name, tmpfile, lei_mics)
                os.unlink(tmpfile)


def latest_full_set(
    context: Context, dump_urls: Iterable[Tuple[str, str]]
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
