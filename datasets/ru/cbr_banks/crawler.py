from lxml import etree
from rigour.mime.types import XML

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.extract.zyte_api import fetch_resource

SOAP_URL = "http://www.cbr.ru/CreditInfoWebServ/CreditOrgInfo.asmx"
SOAP_HEADERS = {"Content-Type": "text/xml; charset=utf-8"}


def send_soap_request(context: Context, action, body, cache_days=None):
    """Sends a SOAP request and returns the parsed XML response."""
    headers = SOAP_HEADERS.copy()
    headers["SOAPAction"] = f"http://web.cbr.ru/{action}"

    response = context.fetch_text(
        SOAP_URL, method="POST", headers=headers, data=body, cache_days=cache_days
    )
    # Make sure we encode as the xml says it is.
    assert "utf-8" in response.split("\n", 1)[0]
    root = etree.fromstring(response.encode("utf-8"))
    return h.remove_namespace(root)


def get_org_info(context: Context, internal_code: str):
    # Create the SOAP request body
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <CreditInfoByIntCodeXML xmlns="http://web.cbr.ru/">
                <InternalCode>{internal_code}</InternalCode>
            </CreditInfoByIntCodeXML>
        </soap:Body>
    </soap:Envelope>"""
    return send_soap_request(context, "CreditInfoByIntCodeXML", body)


def bic_to_int_code(context: Context, bic):
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

    tree = send_soap_request(context, "BicToIntCode", body, cache_days=30)

    # Extract the result
    result = tree.find(".//BicToIntCodeResult")
    if result is not None and result.text != "-1":
        return result.text
    else:
        context.log.info("No internal code found for BIC", bic=bic)
        return None


def crawl_details(context: Context, bic: str, entity: Entity, short_name: str | None):
    """Crawl additional details on best-effort basis and emit."""
    co_data = None
    lic_data = None
    internal_code = bic_to_int_code(context, bic)

    if internal_code is not None:
        result = get_org_info(context, internal_code)
        error = result.findtext(".//CreditInfoByIntCodeXMLResult/Error")
        if error:
            if error == "NotFound":
                # Usually when the license has expired but the bic is still listed
                pass
            else:
                context.log.error(
                    "Error fetching details",
                    internal_code=internal_code,
                    entity_id=entity.id,
                    error=error.text,
                )

        co_data = result.find(".//CreditOrgInfo/CO")
        lic_data = result.find(".//CreditOrgInfo/LIC")

    if co_data is None:
        # This is all caps ugliness - let's only use it when we're not getting nice names
        # from the detailed info.
        h.apply_reviewed_names(context, entity, short_name, enable_llm_cleaning=True)
    else:
        ssv_date = co_data.findtext("SSV_Date")
        reg_date = co_data.findtext("MainDateReg")

        en_names = co_data.findtext("encname")
        h.review_names(context, entity, en_names, enable_llm_cleaning=True)
        h.review_names(
            context, entity, co_data.findtext("OrgName"), enable_llm_cleaning=True
        )
        h.review_names(
            context, entity, co_data.findtext("OrgFullName"), enable_llm_cleaning=True
        )
        h.review_names(
            context, entity, co_data.findtext("csname"), enable_llm_cleaning=True
        )

        phones = co_data.findtext("phones")
        lic_withd_num = co_data.findtext("licwithdnum")
        lic_withd_date = co_data.findtext("licwithddate")
        entity.add("name", co_data.findtext("OrgName"))
        entity.add("name", co_data.findtext("OrgFullName"))
        entity.add("name", co_data.findtext("csname"))
        entity.add("ogrnCode", co_data.findtext("MainRegNumber"))
        entity.add("bikCode", co_data.findtext("BIC"))
        entity.add("registrationNumber", co_data.findtext("RegNumber"))
        entity.add("address", co_data.findtext("UstavAdr"))
        entity.add("address", co_data.findtext("FactAdr"))
        entity.add("amount", co_data.findtext("UstMoney"))
        entity.add("status", co_data.findtext("OrgStatus"))
        entity.add("topics", "fin.bank")
        if en_names is not None:
            for name in en_names.split(","):
                entity.add("name", name, lang="eng")
        if phones is not None:
            phones = h.multi_split(phones, ",")
            for phone in phones:
                if phone.startswith("("):
                    phone = "+7" + phone
                entity.add("phone", phone)
        # source for specifications below: https://www.cbr.ru/Content/Document/File/92046/CreditInfoByIntCodeEx.xsd
        if ssv_date is not None:
            entity.add(
                "notes",
                f"Дата вынесения заключения (признак внесения КО в Систему страхования вкладов): {ssv_date[:10]}",
            )
            entity.add(
                "notes",
                f"Date of the conclusion (sign of inclusion of the FI in the Deposit Insurance System): {ssv_date[:10]}",
                lang="eng",
            )
        if reg_date is not None:
            entity.add(
                "notes",
                f"Дата присвоения государственного регистрационного номера: {reg_date[:10]}",
            )
            entity.add(
                "notes",
                f"Date of assignment of state registration number: {reg_date[:10]}",
                lang="eng",
            )
        if lic_withd_num is not None and lic_withd_date is not None:
            entity.add(
                "status",
                f"Лицензия аннулирована: {lic_withd_num} от {lic_withd_date[:10]}",
            )
            entity.add(
                "status",
                f"License revoked: {lic_withd_num} from {lic_withd_date[:10]}",
                lang="eng",
            )
        h.apply_date(
            entity, "incorporationDate", co_data.findtext("DateKGRRegistration")
        )

    if lic_data is not None:
        license_date = lic_data.findtext("LDate")
        license_code = lic_data.findtext("LCode")
        entity.add("classification", lic_data.findtext("LT"))
        if license_date is not None and license_code is not None:
            entity.add(
                "status",
                f"Код лицензии: {license_code}, дата: {license_date[:10]}",
            )
            entity.add(
                "status",
                f"License code: {license_code}, date: {license_date[:10]}",
                lang="eng",
            )
    entity.add("jurisdiction", "ru")

    context.emit(entity)


def crawl(context: Context):
    # protected by ddos-guard
    _, _, _, path = fetch_resource(
        context,
        "source.xml",
        context.data_url,
        expected_media_type=XML,
    )
    with open(path, "rb") as file:
        doc = etree.fromstring(file.read())
    records = doc.findall(".//Record")
    if not records:
        raise ValueError("No <Record> elements found in the XML.")
    for record in records:
        bic = record.findtext("Bic")
        entity = context.make("Company")
        entity.id = context.make_slug(bic)
        entity.add("ogrnCode", record.findtext("RegNum"))
        entity.add("bikCode", bic)

        crawl_details(context, bic, entity, record.findtext("ShortName"))
