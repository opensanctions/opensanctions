from lxml import etree
import requests

from zavod import Context, helpers as h

HEADERS_MAIN = {
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

SOAP_URL = "http://www.cbr.ru/CreditInfoWebServ/CreditOrgInfo.asmx"
HEADERS = {"Content-Type": "text/xml; charset=utf-8"}
NAMESPACE = {
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
    "ns": "http://web.cbr.ru/",
}


def send_soap_request(action, body):
    """Sends a SOAP request and returns the parsed XML response."""
    headers = HEADERS.copy()
    headers["SOAPAction"] = f"http://web.cbr.ru/{action}"

    response = requests.post(SOAP_URL, data=body, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"Request failed with status {response.status_code}: {response.text}"
        )

    return etree.fromstring(response.content)


def bic_to_int_code(bic):
    """Gets the internal code for a BIC."""
    # Formulate the SOAP request body
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <BicToIntCode xmlns="http://web.cbr.ru/">
                <BicCode>{bic}</BicCode>
            </BicToIntCode>
        </soap:Body>
    </soap:Envelope>"""

    tree = send_soap_request("BicToIntCode", body)

    # Extract the result
    result = tree.find(".//ns:BicToIntCodeResult", namespaces=NAMESPACE)
    if result is not None:
        return result.text
    else:
        print("Result not found in the response")
        return None


def credit_info_by_int_code(internal_code):
    """Gets detailed credit information for an internal code."""
    # Create the SOAP request body
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <CreditInfoByIntCodeXML xmlns="http://web.cbr.ru/">
                <InternalCode>{internal_code}</InternalCode>
            </CreditInfoByIntCodeXML>
        </soap:Body>
    </soap:Envelope>"""

    tree = send_soap_request("CreditInfoByIntCodeXML", body)

    # Locate the main result element
    result = tree.find(".//ns:CreditInfoByIntCodeXMLResult", namespaces=NAMESPACE)
    if result is not None:
        credit_org_info = result.find("CreditOrgInfo")
        if credit_org_info is not None:
            # Extract elements within CO and LIC
            co_data = credit_org_info.find("CO")
            lic_data = credit_org_info.find("LIC")

            if co_data is not None:
                for element in co_data:
                    print(f"{element.tag}: {element.text}")
            if lic_data is not None:
                for element in lic_data:
                    print(f"{element.tag}: {element.text}")
        else:
            print("CreditOrgInfo not found in the response")
    else:
        print("CreditInfoByIntCodeXMLResult not found in the response")


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url, headers=HEADERS_MAIN)
    with open(path, encoding="windows-1251") as file:
        xml_content = file.read()
    doc = etree.fromstring(xml_content.encode("windows-1251"))
    records = doc.findall(".//Record")
    if not records:
        context.log.warning("No <Record> elements found in the XML.")
        return
    for record in records:
        du_date = record.get("DU")
        reg_date = record.find("RegNum").get("date")
        bic = record.findtext("Bic")
        name = record.findtext("ShortName")
        reg_num = record.findtext("RegNum")
        entity = context.make("Company")
        entity.id = context.make_slug(bic)
        entity.add("name", name)
        entity.add("ogrnCode", reg_num)
        entity.add("bikCode", bic)
        h.apply_date(entity, "incorporationDate", reg_date)
        h.apply_date(entity, "modifiedAt", du_date)
        context.emit(entity)
        code = bic_to_int_code(bic)
        print(code)
        data = credit_info_by_int_code(code)
        print(data)
