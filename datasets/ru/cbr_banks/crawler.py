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


def details_by_int_code(internal_code):
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

    details = {}
    # Locate the main result element
    result = tree.find(".//ns:CreditInfoByIntCodeXMLResult", namespaces=NAMESPACE)
    if result is not None:
        credit_org_info = result.find("CreditOrgInfo")
        if credit_org_info is not None:
            # Extract elements within CO
            co_data = credit_org_info.find("CO")
            if co_data is not None:
                details.update(
                    {
                        "reg_number": co_data.findtext("RegNumber"),
                        "reg_code": co_data.findtext("RegCode"),  # not used
                        "bic": co_data.findtext("BIC"),
                        "org_name": co_data.findtext("OrgName"),
                        "full_name": co_data.findtext("OrgFullName"),
                        "legal_name": co_data.findtext("csname"),
                        "en_name": co_data.findtext("encname"),
                        "phones": co_data.findtext("phones"),
                        "inc_date": co_data.findtext("DateKGRRegistration"),
                        "ogrn": co_data.findtext("MainRegNumber"),
                        "main_reg": co_data.findtext("MainDateReg"),
                        "address1": co_data.findtext("UstavAdr"),
                        "address2": co_data.findtext("FactAdr"),
                        "capital": co_data.findtext("UstMoney"),
                        "status": co_data.findtext("OrgStatus"),
                        "ssv_date": co_data.findtext("SSV_Date"),
                        "lic_withd_num": co_data.findtext("licwithdnum"),
                        "lic_withd_date": co_data.findtext("licwithddate"),
                        # "ruleactual": co_data.findtext("ruleactual"),
                        # "cdmoney": co_data.findtext("cdmoney"),
                    }
                )

            # Extract elements within LIC
            lic_data = credit_org_info.find("LIC")
            if lic_data is not None:
                details.update(
                    {
                        "LT": lic_data.findtext("LT"),
                        "LDate": lic_data.findtext("LDate"),
                    }
                )
        else:
            print("CreditOrgInfo not found in the response")
    else:
        print("CreditInfoByIntCodeXMLResult not found in the response")

    return details


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
        bic = record.findtext("Bic")
        entity = context.make("Company")
        entity.id = context.make_slug(bic)
        int_code = bic_to_int_code(bic)
        details = details_by_int_code(int_code)
        if details:
            ssv_date = details.pop("ssv_date")
            reg_date = details.pop("main_reg")
            en_names = details.pop("en_name")
            phones = details.pop("phones")
            lic_withd_num = details.pop("lic_withd_num")
            lic_withd_date = details.pop("lic_withd_date")
            entity.add("name", details.pop("org_name"))
            entity.add("name", details.pop("full_name"))
            entity.add("name", details.pop("legal_name"))
            entity.add("ogrnCode", details.pop("ogrn"))
            entity.add("bikCode", details.pop("bic"))
            entity.add("registrationNumber", details.pop("reg_number"))
            entity.add("address", details.pop("address1"))
            entity.add("address", details.pop("address2"))
            entity.add("amount", details.pop("capital"))
            entity.add("status", details.pop("status"))
            if en_names is not None:
                for name in en_names.split(","):
                    entity.add("name", name, lang="eng")
            if phones is not None:
                for phone in phones.split(","):
                    entity.add("phone", phone)
            if ssv_date is not None:
                entity.add(
                    "notes",
                    f"Дата вынесения заключения (признак внесения КО в Систему страхования вкладов): {ssv_date[:10]}",
                )
            if reg_date is not None:
                entity.add(
                    "notes",
                    f"Дата присвоения государственного регистрационного номера: {reg_date[:10]}",
                )
            if lic_withd_num is not None and lic_withd_date is not None:
                entity.add(
                    "status",
                    f"Лицензия аннулирована: {lic_withd_num} от {lic_withd_date[:10]}",
                )
            h.apply_date(
                entity, "incorporationDate", details.get("DateKGRRegistration")
            )
            context.emit(entity)
        else:
            context.log.warning(f"No details found for BIC {bic}")
