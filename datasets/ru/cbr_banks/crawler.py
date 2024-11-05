from lxml import etree
import requests

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


def bic_to_int_code(bic):
    url = "http://www.cbr.ru/CreditInfoWebServ/CreditOrgInfo.asmx"

    # Formulate the SOAP request body
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <BicToIntCode xmlns="http://web.cbr.ru/">
                <BicCode>{bic}</BicCode>
            </BicToIntCode>
        </soap:Body>
    </soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://web.cbr.ru/BicToIntCode",
    }

    # Send the request
    response = requests.post(url, data=body, headers=headers)

    # Check if the request was successful
    if response.status_code != 200:
        print(f"Request failed with status {response.status_code}")
        return None

    # Parse the XML response
    tree = etree.fromstring(response.content)
    namespace = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "ns": "http://web.cbr.ru/",
    }

    # Assume the result is in the BicToIntCodeResult tag
    result = tree.find(".//ns:BicToIntCodeResult", namespaces=namespace)
    if result is not None:
        code = result.text
        return code
    else:
        print("Result not found in the response")
        return None


# bic = "040173771"
# code = bic_to_int_code(bic)
# print(code)


def credit_info_by_int_code(internal_code):
    url = "http://www.cbr.ru/CreditInfoWebServ/CreditOrgInfo.asmx"

    # Create the SOAP request body
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <CreditInfoByIntCodeExXML xmlns="http://web.cbr.ru/">
                <InternalCode>{internal_code}</InternalCode>
            </CreditInfoByIntCodeExXML>
        </soap:Body>
    </soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://web.cbr.ru/CreditInfoByIntCodeExXML",
    }

    response = requests.post(url, data=body, headers=headers)
    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(
            f"Request failed with status {response.status_code}: {response.text}"
        )

    # Parse the XML response
    tree = etree.fromstring(response.content)
    namespace = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "ns": "http://web.cbr.ru/",
    }

    # Extract the result
    result = tree.find(".//ns:CreditInfoByIntCodeExXMLResult", namespaces=namespace)
    if result is not None:
        # Convert the XML element to a string, stripping any whitespace
        xml_data = (
            etree.tostring(result, encoding="utf-8", method="text")
            .decode("utf-8")
            .strip()
        )
        print(xml_data)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url, headers=HEADERS)
    with open(path, encoding="windows-1251") as file:
        xml_content = file.read()
    doc = etree.fromstring(xml_content.encode("windows-1251"))
    records = doc.findall(".//Record")
    if not records:
        context.log.warning("No <Record> elements found in the XML.")
        return
    bics = set()
    for record in records:
        du_date = record.get("DU")
        reg_date = record.find("RegNum").get("date")
        bic = record.findtext("Bic")
        # bics.add(bic)
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

        # codes = set()
        # for bic in bics:
        code = bic_to_int_code(bic)
        print(code)
        # codes.add(code)
        # for code in codes:
        data = credit_info_by_int_code(code)
        print(data)
        # print(data)
        # print(data)
