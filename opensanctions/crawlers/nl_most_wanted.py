from opensanctions.core import Context
from dateutil.parser import ParserError, parse
from opensanctions import helpers as h

base_url = "https://www.politie.nl/"

FORMATS = ("%d/%m/%Y",)


def crawl_person(context, url, index_crawl_pages, end_of_page, first_person):
    doc = context.fetch_html(url)

    for x in doc.findall(".//section/ul/li"):

        name = x.find(".//h3").text_content().title()
        href = x.xpath(".//a")[0].get("href")

        person = context.make("Person")
        person.id = context.make_slug(name)
        person.add("topics", "crime")
        person.add("sourceUrl", (base_url + href))
        first_name, last_name = name.split(" ", 1)
        person.add("firstName", first_name)
        person.add("lastName", last_name)

        docPerson = context.fetch_html(base_url + href)
        i = 0

        for x in docPerson.findall('.//dl[@id="gegevens-title-dl"]/dt'):
            j = 0
            i = i + 1
            if x.text_content().title() == "Date Of Birth:":
                for y in docPerson.findall(
                    './/dl[@id="gegevens-title-dl"]/dd'
                ):
                    j = j + 1
                    if j == i:
                        try:
                            parsed_date = parse(
                                y.text_content().title()
                            ).strftime("%d/%m/%Y")
                            person.add(
                                "birthDate",
                                h.parse_date(parsed_date, FORMATS),
                            )
                        except ParserError:
                            pass

            if x.text_content().title() == "Nationality:":
                for y in docPerson.findall(
                    './/dl[@id="gegevens-title-dl"]/dd'
                ):
                    j = j + 1
                    if j == i:
                        person.add("nationality", y.text_content().title())

            if x.text_content().title() == "Sex:":
                for y in docPerson.findall(
                    './/dl[@id="gegevens-title-dl"]/dd'
                ):
                    j = j + 1
                    if j == i:
                        person.add("gender", y.text_content().title())

        if first_person != name and not end_of_page:
            context.emit(person, target=True)
        else:
            end_of_page = True

        if first_person == "":
            first_person = name

    crawl_pages(context, index_crawl_pages, end_of_page, first_person)


def crawl_pages(context, index_crawl_pages, end_of_page, first_person):
    if not end_of_page:
        url = (
            "https://www.politie.nl/en/wanted-and-missing/most-wanted?page="
            + str(index_crawl_pages)
        )
        index_crawl_pages = index_crawl_pages + 1
        crawl_person(
            context, url, index_crawl_pages, end_of_page, first_person
        )


def crawl(context: Context):
    crawl_pages(context, 1, False, "")
