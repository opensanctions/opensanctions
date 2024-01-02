from zavod import Context
from lxml import html, etree

def parce_entity(context: Context, element):
    # Initialize an empty dictionary to store the extracted data
    entity_data = {}

    # Extract the name. Assumes the name is in the second <td>, within an <a> tag
    name = element.xpath('.//td[2]/a/text()')
    entity_data['name'] = name[0].strip() if name else 'Unknown'

    url_extension = element.xpath('.//td[2]/a/@href')
    entity_data['url_extension'] = url_extension[0].strip() if url_extension else 'Unknown'

    # Extract other details - district, term, term start and end dates, and block
    # Adjust the indices of td elements based on the structure of your HTML
    district = element.xpath('.//td[3]/text()')
    entity_data['district'] = district[0].strip() if district else 'Unknown'

    term = element.xpath('.//td[4]/text()')
    entity_data['term'] = term[0].strip() if term else 'Unknown'

    term_start = element.xpath('.//td[5]/text()')
    entity_data['term_start'] = term_start[0].strip() if term_start else 'Unknown'

    term_end = element.xpath('.//td[6]/text()')
    entity_data['term_end'] = term_end[0].strip() if term_end else 'Unknown'

    block = element.xpath('.//td[7]/text()')
    entity_data['block'] = block[0].strip() if block else 'Unknown'

    return entity_data

def crawl_entity(context: Context, entity):
    entity_url = context.dataset.data.url + entity['url_extension'].replace('/diputados/','')
    context.log.info(f"Starting crawling entity {entity_url}")
    response = context.http.get(entity_url)

    # Parse the HTTP response into an lxml DOM:
    doc = html.fromstring(response.text)

    save_text_to_file(doc, f"/opensanctions/data/{entity['name'].split(',')[0]}.txt")
    context.log.info("Saved output.txt")

def save_text_to_file(element, filename):
    text = etree.tostring(element, pretty_print=True, method="html").decode()
    with open(filename, 'w') as file:
        file.write(text)

def crawl(context: Context):
    context.log.info("Starting crawling")
    response = context.http.get(context.dataset.data.url)

    # Parse the HTTP response into an lxml DOM:
    doc = html.fromstring(response.text)

    # Find all <tr> elements within <tbody>
    for element in doc.xpath('//tbody/tr')[:10]:
        ent = parce_entity(context, element)
        context.log.info(f"parsed entity: {ent}")
        crawl_entity(context, ent)

    context.log.info(f"This is 100 symbols of doc {doc[:100]}")

    save_text_to_file(doc, '/opensanctions/data/output.txt')
    context.log.info("Saved output.txt")
