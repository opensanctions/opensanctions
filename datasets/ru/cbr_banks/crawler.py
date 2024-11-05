from lxml import etree

from zavod import Context, helpers as h

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Cookie": "__ddg2_=oODOe2gVFAHstFSz; __ddg5_=5VLoRhvMkPVMHI4H; __ddgid_=F69Zw7VH7FdLn3xt; __ddgmark_=C04rsUQPh6Z7r9Xw; _ym_d=1730798853; _ym_isad=2; _ym_uid=1730798853821838136; __ddg1_=FwX17Njle4Mnl9WOR8V5",
    "Host": "cbr.ru",
    "Referer": "https://cbr.ru/scripts/XML_bic2.asp",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
}

HEADERS_BIC_TO_INT = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Cookie": "__ddg10_=1730823041; __ddg8_=Id68HeA2kUV7ZDIG; __ddg9_=62.80.98.154; _ym_visorc=b; __ddg2_=oODOe2gVFAHstFSz; __ddgid_=F69Zw7VH7FdLn3xt; __ddgmark_=C04rsUQPh6Z7r9Xw; accept=1; _ym_d=1730798853; _ym_isad=2; _ym_uid=1730798853821838136; ASPNET_SessionID=miiyxxjyhin1pcfh3c5ohfx5; __ddg1_=FwX17Njle4Mnl9WOR8V5",
    "Host": "www.cbr.ru",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
}

BIC_TO_INT_CODE_REQUEST = """
<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <BicToIntCode xmlns="http://web.cbr.ru/">
      <BicCode>"040173771"</BicCode>
    </BicToIntCode>
  </soap:Body>
</soap:Envelope>
"""

REQUEST_DATA = """
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CreditInfoByIntCodeExXML xmlns="http://web.cbr.ru/">
      <InternalCodes>
        <double></double>
      </InternalCodes>
    </CreditInfoByIntCodeExXML>
  </soap:Body>
</soap:Envelope>
"""

HEADERS_SOAP = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": "http://web.cbr.ru/BicToIntCode",
}


def crawl(context: Context) -> None:
    # HEADERS_SOAP["Content-Length"] = str(len(BIC_TO_INT_CODE_REQUEST.encode("utf-8")))

    response = context.fetch_text(
        url="https://www.cbr.ru/CreditInfoWebServ/CreditOrgInfo.asmx",
        headers=HEADERS_SOAP,
        data=BIC_TO_INT_CODE_REQUEST,
        method="GET",
    )
    print(response)

    # tree = etree.fromstring(response.encode("utf-8"))
    # print(tree)
    # Use the namespace to locate the result
    namespace = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "ns": "http://web.cbr.ru/",
    }
    # result = tree.find(".//ns:BicToIntCodeResult", namespaces=namespace)

    # if result is not None:
    #     print("BicToIntCodeResult:", result.text)
    # else:
    #     context.log.warning("BicToIntCodeResult not found in the response.")


# def crawl(context: Context):
#     path = context.fetch_resource("source.xml", context.data_url, headers=HEADERS)
#     with open(path, encoding="windows-1251") as file:
#         xml_content = file.read()
#     doc = etree.fromstring(xml_content.encode("windows-1251"))
#     records = doc.findall(".//Record")
#     if not records:
#         context.log.warning("No <Record> elements found in the XML.")
#         return
#     for record in records:
#         du_date = record.get("DU")
#         reg_date = record.find("RegNum").get("date")
#         bic = record.findtext("Bic")
#         name = record.findtext("ShortName")
#         reg_num = record.findtext("RegNum")
#         entity = context.make("Company")
#         entity.id = context.make_slug(bic)
#         entity.add("name", name)
#         entity.add("ogrnCode", reg_num)
#         entity.add("bikCode", bic)
#         h.apply_date(entity, "incorporationDate", reg_date)
#         h.apply_date(entity, "modifiedAt", du_date)
#         context.emit(entity)
