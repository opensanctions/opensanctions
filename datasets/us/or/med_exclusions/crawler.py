from lxml import etree

from zavod import Context, helpers as h

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

    response = context.fetch_text(
        context.data_url, headers=HEADERS, data=REQUEST_DATA, method="POST"
    )

    tree = etree.fromstring(response.encode("utf-8"))

    for item in [
        row.get("ows_URL")
        for row in tree.findall(".//rs:data/z:row", namespaces=NAMESPACES)
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
