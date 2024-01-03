from lxml import html, etree
from datetime import datetime

from zavod import Context
from zavod import helpers as h
# from zavod.logic.pep import categorise

def crawl_person(context: Context, element):
    person = context.make("Person")

    # Extract PEP's name, create id and add first properties
    name = element.xpath('.//td[2]/a/text()')
    if name:
        person.id = context.make_id(name[0].strip())
        context.log.info(f"Made person's ID: {person}: {person.id}")
        person.add('name', name[0].strip())
        person.add('alias', f"{name[0].strip().split(',')[1]} {name[0].strip().split(',')[0]}")
        person.add("country", "ar")
        h.apply_name(
            person,
            first_name=name[0].strip().split(',')[1],
            last_name=name[0].strip().split(',')[0],
        )

    # Extract PEP's image url // TODO: find out if we need term
    element_image_url = element.xpath('//tr/td[1]/img/@src')
    image_url = element_image_url[0].strip() if element_image_url else None

    # Extract link to PEP's personal page
    url_extension = element.xpath('.//td[2]/a/@href')
    if url_extension:
        url = context.dataset.data.url + url_extension[0].strip().replace('/diputados/', '')
        person.add("sourceUrl", url)
        context.log.info(f"Added personal page as URL for {person}")
        crawl_personal_page(context, url, person)
    else:
        person.add("sourceUrl", context.dataset.data.url)
        context.log.info(f"Added Parliament page as URL for {person}")

    # Extract district and create position
    district = element.xpath('.//td[3]/text()')
    if district:
        position = h.make_position(
            context,
            name="Member of National Congress of Argentina",
            country="ar",
            subnational_area=district[0].strip(),
            # topics=[] TODO: find out what is topic and if needed
        )
    else:
        position = h.make_position(
            context,
            name="Member of National Congress of Argentina",
            country="ar",
            # topics=[] TODO: find out what is topic and if needed
        )

    # Extract term // TODO: find out if we need term
    element_term = element.xpath('.//td[4]/text()')
    term = element_term[0].strip() if element_term else None

    # Extract term start and end dates and add them to occupancy
    element_term_start = element.xpath('.//td[5]/text()')
    term_start = element_term_start[0].strip() if element_term_start else None
    element_term_end = element.xpath('.//td[6]/text()')
    term_end = element_term_end[0].strip() if element_term_end else None
    # categorisation = categorise(context, position, True) # TODO: find out what is categorise and if we need it
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        start_date=term_start,
        end_date=term_end,
        # categorisation=categorisation,
    )

    # Extract political block and add to person
    block = element.xpath('.//td[7]/text()')
    if block:
        person.add("political", block[0].strip())

    context.log.info(f"enriched entity: {person}")
    context.emit(person, target=True)

def crawl_personal_page(context: Context, url, person):
    context.log.info(f"Starting crawling entity {url}")
    response = context.http.get(url)

    # Parse the HTTP response into an lxml DOM:
    doc = html.fromstring(response.text)

    # Extract profession // TODO: find out if we need and can use profession
    profession = doc.xpath('.//p[@class="encabezadoProfesion"]/span/text()')
    entity_profession = profession[0].strip() if profession else None

    # Extract date of birth
    birth_date = doc.xpath('.//p[@class="encabezadoFecha"]/span/text()')
    if birth_date:
        person.add('birthDate', convert_date_format(birth_date[0].strip()))

    # Extract email
    email = doc.xpath('.//a[starts-with(@href, "mailto:")]/text()')
    if email:
        person.add('email', email[0].strip())

def save_text_to_file(element, filename):
    text = etree.tostring(element, pretty_print=True, method="html").decode()
    with open(filename, 'w') as file:
        file.write(text)

def convert_date_format(date_string):
    # Parse the date string into a datetime object
    date_obj = datetime.strptime(date_string, '%d/%m/%Y')

    # Format the datetime object into the desired format
    return date_obj.strftime('%Y-%m-%d')

def crawl(context: Context):
    context.log.info("Starting crawling")
    response = context.http.get(context.dataset.data.url)

    # Parse the HTTP response into an lxml DOM:
    doc = html.fromstring(response.text)
    context.log.info(f"This is 100 symbols of doc {doc[:100]}")

    save_text_to_file(doc, '/opensanctions/data/output.txt')
    context.log.info("Saved output.txt")

    # Find all <tr> elements within <tbody>
    for element in doc.xpath('//tbody/tr')[:10]:
        crawl_person(context, element)


