from datetime import datetime
from typing import Optional, List

from normality import slugify, stringify
from openpyxl import load_workbook

from rigour.mime.types import XLSX
from zavod import Context
from zavod import helpers as h

# From inspecting the getNewAccessToken function in the website source, the UUID corresponds to the report ID,
# so hopefully won't change too quickly.
GET_TOKEN_URL = "https://www.iadb.org/en/idb_powerbi_refresh_token/0a2a387d-99f5-4bd7-a6f0-78299d886d79"

# Request data, obtained by using "Copy POST Data" in the browser
REQUEST_DATA = b"""{"exportDataType":2,"executeSemanticQueryRequest":{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"s","Entity":"Sanctions","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Title"},"Name":"Sanctions.Title","NativeReferenceName":"Title"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Entity"},"Name":"Sanctions.Entity","NativeReferenceName":"Entity"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Nationality"},"Name":"Sanctions.Nationality","NativeReferenceName":"Nationality"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Country"},"Name":"Sanctions.Country","NativeReferenceName":"Country"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Grounds"},"Name":"Sanctions.Grounds","NativeReferenceName":"Grounds"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Source"},"Name":"Sanctions.Source","NativeReferenceName":"Source"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"IDB Sanction Type"},"Name":"Sanctions.IDB Sanction Type","NativeReferenceName":"IDB Sanction Type"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"IDB Sanction Source"},"Name":"Sanctions.IDB Sanction Source","NativeReferenceName":"IDB Sanction Source"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Other Name"},"Name":"Sanctions.Other Name","NativeReferenceName":"Other Name"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"From"},"Name":"Sanctions.From","NativeReferenceName":"From1"},{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"To"},"Name":"Sanctions.To","NativeReferenceName":"To1"}],"Where":[{"Condition":{"Comparison":{"ComparisonKind":2,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"From Date"}},"Right":{"Literal":{"Value":"datetime'2007-03-23T00:00:00'"}}}}}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"s"}},"Property":"Title"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6,7,8,9,10],"Subtotal":1}]},"DataReduction":{"Primary":{"Top":{"Count":1000000}},"Secondary":{"Top":{"Count":100}}},"Version":1}}},{"ExportDataCommand":{"Columns":[{"QueryName":"Sanctions.Title","Name":"Title"},{"QueryName":"Sanctions.Entity","Name":"Entity"},{"QueryName":"Sanctions.Nationality","Name":"Nationality"},{"QueryName":"Sanctions.Country","Name":"Country"},{"QueryName":"Sanctions.Grounds","Name":"Grounds"},{"QueryName":"Sanctions.Source","Name":"Source"},{"QueryName":"Sanctions.IDB Sanction Type","Name":"IDB Sanction Type"},{"QueryName":"Sanctions.IDB Sanction Source","Name":"IDB Sanction Source"},{"QueryName":"Sanctions.Other Name","Name":"Other Name"},{"QueryName":"Sanctions.From","Name":"From"},{"QueryName":"Sanctions.To","Name":"To"}],"Ordering":[0,1,2,3,9,10,4,5,6,7,8],"FiltersDescription":"Applied filters:\nFrom Date is on or after March 23, 2007"}}]}}],"cancelQueries":[],"modelId":8885050,"userPreferredLocale":"en-US"},"artifactId":9296919})"""


def parse_countries(countries: Optional[str]) -> List[str]:
    parsed: List[str] = []
    if countries is None:
        return parsed
    for country in countries.split(", "):
        country = country.strip()
        if len(country):
            parsed.append(country)
    return parsed


def header_names(cells):
    headers = []
    for idx, cell in enumerate(cells):
        if cell is None:
            cell = f"column_{idx}"
        headers.append(slugify(cell, "_"))
    return headers


def excel_records(path):
    wb = load_workbook(filename=path, read_only=True)
    for sheet in wb.worksheets:
        headers = None
        for idx, row in enumerate(sheet.rows):
            cells = [c.value for c in row]
            if headers is None:
                headers = header_names(cells)
                continue
            record = {}
            for header, value in zip(headers, cells):
                if isinstance(value, datetime):
                    value = value.date()
                value = stringify(value)
                if value is not None:
                    record[header] = value
            yield record


def crawl(context: Context):
    # The IADB PowerBI report requires a token to access the data, which it luckily readily provides.
    get_token_response = context.fetch_json(GET_TOKEN_URL)
    token = get_token_response["token"]["token"]
    path = context.fetch_resource(
        "data.xlsx",
        context.data_url,
        method="POST",
        data=REQUEST_DATA,
        headers={
            "Authorization": f"EmbedToken {token}",
            "Content-Type": "application/json",
        },
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    for row in excel_records(path):
        # They mis-classify some entities so we don't use this for schema.
        entity_type = row.pop("entity", None)
        if entity_type is None:
            continue
        title = row.pop("title")
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(entity_type, title)
        entity.add("name", title)
        entity.add("alias", row.pop("other_name", None))
        entity.add("country", parse_countries(row.pop("country", None)))
        for country in parse_countries(row.pop("nationality", None)):
            entity.add("country", country)

        sanction = h.make_sanction(context, entity)
        # sanction.add("status", row.pop("statusName"))
        sanction.add("reason", row.pop("grounds", None))
        sanction.add("authority", row.pop("source", None))
        sanction.add("authority", row.pop("idb_sanction_source", None))
        sanction.add("program", row.pop("idb_sanction_type", None))
        h.apply_date(sanction, "startDate", row.pop("from", None))
        # Sometimes row.to is "Ongoing", which will be datapatched to null end_date
        h.apply_date(sanction, "endDate", row.pop("to", None))

        is_debarred = h.is_active(sanction)
        if is_debarred:
            entity.add("topics", "debarment")

        context.emit(sanction)
        context.emit(entity)
        context.audit_data(row)
