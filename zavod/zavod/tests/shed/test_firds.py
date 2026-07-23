from lxml import etree
import requests_mock
from structlog.testing import capture_logs

from zavod.context import Context
from zavod.meta import Dataset
from zavod.shed.firds import MIC_REGISTRY_URL, load_lei_mics, parse_element

MIC_CSV = (
    '"MIC","OPERATING MIC","OPRT/SGMT","MARKET NAME-INSTITUTION DESCRIPTION",'
    '"LEGAL ENTITY NAME","LEI","MARKET CATEGORY CODE","ACRONYM",'
    '"ISO COUNTRY CODE (ISO 3166)","CITY","WEBSITE","STATUS","CREATION DATE",'
    '"LAST UPDATE DATE","LAST VALIDATION DATE","EXPIRY DATE","COMMENTS"\r\n'
    '"TPIR","TPIC","SGMT","TP ICAP EU - MTF - REGISTRATION","TP ICAP (EUROPE) SA",'
    '"213800R54EFFINMY1P02","MLTF",,"FR","PARIS","WWW.TPICAP.COM","ACTIVE",'
    '"20180924","20190225","","","MULTILATERAL TRADING FACILITY."\r\n'
    '"TPIC","TPIC","OPRT","TP ICAP EU - MTF","TP ICAP (EUROPE) SA",'
    '"213800R54EFFINMY1P02","MLTF",,"FR","PARIS","WWW.TPICAP.COM","ACTIVE",'
    '"20180924","20190225","","",""\r\n'
    '"XCNQ","XCNQ","OPRT","CANADIAN SECURITIES EXCHANGE","CNSX MARKETS, INC.",'
    '"","RMKT","CSE LISTED","CA","TORONTO","WWW.THECSE.COM","ACTIVE",'
    '"20090427","20210927","20210927","",""\r\n'
)

REF_DATA_XML = """
<RefData xmlns="urn:iso:std:iso:20022:tech:xsd:auth.017.001.02">
  <FinInstrmGnlAttrbts>
    <Id>{isin}</Id>
    <FullNm>Test Instrument</FullNm>
    <ClssfctnTp>EDSXFR</ClssfctnTp>
  </FinInstrmGnlAttrbts>
  <Issr>{lei}</Issr>
  <TechAttrbts>
    <RlvntCmptntAuthrty>FR</RlvntCmptntAuthrty>
    <RlvntTradgVn>{mic}</RlvntTradgVn>
  </TechAttrbts>
</RefData>
"""


def test_load_lei_mics(testdataset1: Dataset):
    context = Context(testdataset1)
    with requests_mock.Mocker() as m:
        m.get(MIC_REGISTRY_URL, text=MIC_CSV)
        lei_mics = load_lei_mics(context)
    # Rows without an LEI are skipped, segment and operating MICs are grouped.
    assert lei_mics == {"213800R54EFFINMY1P02": {"TPIR", "TPIC"}}
    context.close()


def test_parse_element_logs_venue_operator_issuer(testdataset1: Dataset):
    context = Context(testdataset1)
    lei_mics = {"213800R54EFFINMY1P02": {"TPIR", "TPIC"}}

    # The Issr LEI operates the record's relevant trading venue:
    elem = etree.fromstring(
        REF_DATA_XML.format(isin="US3682872078", lei="213800R54EFFINMY1P02", mic="TPIR")
    )
    with capture_logs() as cap_logs:
        parse_element(context, "test.xml", elem, lei_mics)
    assert {
        "event": "Issr LEI is the operator of the relevant trading venue",
        "log_level": "info",
        "isin": "US3682872078",
        "lei": "213800R54EFFINMY1P02",
        "mic": "TPIR",
    } in cap_logs
    # The issuer is still emitted and linked, not dropped:
    assert context.stats.entities == 2

    # An unrelated issuer LEI is not logged:
    elem = etree.fromstring(
        REF_DATA_XML.format(isin="GB00B71N6K86", lei="5493005B7DAN39RXLK23", mic="TPIR")
    )
    with capture_logs() as cap_logs:
        parse_element(context, "test.xml", elem, lei_mics)
    assert cap_logs == []
    assert context.stats.entities == 4

    context.close()
