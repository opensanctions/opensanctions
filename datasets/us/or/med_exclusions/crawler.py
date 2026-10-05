from lxml import etree

from zavod import Context, helpers as h
from zavod.extract import zyte_api

REQUEST_DATA = """
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <soap:Body>
            <GetListItems xmlns="http://schemas.microsoft.com/sharepoint/soap/">
                <listName>Documents</listName>
                <viewName>{9070D930-A5F7-4AFC-9EF4-943645B8E724}</viewName>
                <queryOptions>
                    <QueryOptions>
                        <IncludeAttachmentUrls>TRUE</IncludeAttachmentUrls>
                    </QueryOptions>
                </queryOptions>
            </GetListItems>
        </soap:Body>
    </soap:Envelope>
"""
HEADERS = {"Content-Type": "text/xml;charset='utf-8'"}
NAMESPACES = {
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
    "m": "http://schemas.microsoft.com/sharepoint/soap/",
    "rs": "urn:schemas-microsoft-com:rowset",
    "z": "#RowsetSchema",
}


def crawl(context: Context) -> None:
    # www.oregon.gov fails to resolve in the production environment, so the
    # request goes through Zyte, which resolves the host on its own network.
    result = zyte_api.fetch(
        context,
        zyte_api.ZyteAPIRequest(
            url=context.data_url,
            method="POST",
            body=REQUEST_DATA.encode("utf-8"),
            headers=HEADERS,
        ),
    )
    assert result.status_code == 200, result.status_code
    assert result.media_type == "text/xml", result.media_type

    tree = etree.fromstring(result.response_text.encode("utf-8"))

    for item in [
        val
        for row in tree.findall(".//rs:data/z:row", namespaces=NAMESPACES)
        if (val := row.get("ows_URL")) is not None
    ]:
        url, last_name, first_name = item.split(", ")
        person = context.make("Person")
        person.id = context.make_id(first_name, last_name, url)
        h.apply_name(person, first_name=first_name, last_name=last_name)
        person.add("topics", "debarment")
        person.add("country", "us")
        sanction = h.make_sanction(context, person)
        sanction.add("sourceUrl", url)

        context.emit(person)
        context.emit(sanction)
