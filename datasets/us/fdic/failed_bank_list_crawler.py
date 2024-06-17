from zavod import Context
import requests
from lxml import html

def crawl(context: Context):
    context.log.info("Fetching data from FDIC Failed Bank List")
    url = context.dataset.data.url
    response = requests.get(url)
    if response.status_code == 200:
        context.log.info("Data fetched successfully")
        doc = html.fromstring(response.content)
        context.log.info(f"HTML Content: {html.tostring(doc, pretty_print=True)}")
        parse_data(context, doc)
    else:
        context.log.error(f"Failed to fetch data, status code: {response.status_code}")

def parse_data(context, doc):
    context.log.info("Parsing data")
    elements = doc.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "position-relative", " " ))] | //td | //*[contains(concat( " ", @class, " " ), concat( " ", "bg-blue", " " ))]')
    context.log.info(f"Found {len(elements)} elements")
    
    current_row = {}
    for el in elements:
        context.log.info(f"Element content: {html.tostring(el, pretty_print=True)}")
        text_content = el.text_content().strip()
        column_position = el.xpath('count(preceding-sibling::*) + 1')
        context.log.info(f"Processing element in column position: {column_position} with text: {text_content}")

        if column_position == 1:
            if current_row:  # If there's already data, emit it as a previous row
                emit_row(context, current_row)
            current_row = {'bank_name': text_content}
        elif column_position == 2:
            current_row['city'] = text_content
        elif column_position == 3:
            current_row['state'] = text_content
        elif column_position == 4:
            current_row['cert_number'] = text_content
        elif column_position == 5:
            current_row['acquiring_institution'] = text_content
        elif column_position == 6:
            current_row['closing_date'] = text_content
        elif column_position == 7:
            current_row['fund'] = text_content
            emit_row(context, current_row)
            current_row = {}  # Reset for next row

    # Emit last row if not already emitted
    if current_row:
        emit_row(context, current_row)

def emit_row(context, row):
    if len(row) == 7:  # Ensure all fields are present
        entity = context.make("LegalEntity")
        entity.id = context.make_id(row['bank_name'], row['closing_date'])
        entity.add("name", row['bank_name'])
        entity.add("address", ", ".join([row['city'], row['state']]))
        entity.add("description", f"Cert number {row['cert_number']}")
        #entity.add("acquiringInstitution", row['acquiring_institution'])
        entity.add("dissolutionDate", row['closing_date'])
        entity.add("description", f"Fund: {row['fund']}")
        context.emit(entity, target=True)
        context.log.info(f"Emitted entity for bank: {row['bank_name']}")
    else:
        context.log.warning(f"Incomplete row data: {row}")
