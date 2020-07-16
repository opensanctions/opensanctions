from urllib.parse import urljoin
from pprint import pprint  # noqa
from normality import collapse_spaces, stringify
from ftmstore.memorious import EntityEmitter


def element_text(el):
    if el is None:
        return
    text = stringify(el.text_content())
    if text is not None:
        return collapse_spaces(text)


def parse(context, data):
    emitter = EntityEmitter(context)
    url = data.get("url")
    country = data.get("country")
    with context.http.rehash(data) as res:
        doc = res.html
        # updated_at = doc.findtext('.//span[@id="lastUpdateDate"]')
        output = doc.find('.//div[@id="countryOutput"]')
        if output is None:
            return
        # component = None
        for row in output.findall(".//li"):
            # next_comp = row.findtext('./td[@class="componentName"]/strong')
            # if next_comp is not None:
            #     component = next_comp
            #     continue
            function = element_text(row.find('.//span[@class="title"]'))
            if function is None:
                continue
            name = element_text(row.find('.//span[@class="cos_name"]'))
            if name is None:
                continue

            person = emitter.make("Person")
            person.make_id(url, country, name, function)
            person.add("name", name)
            person.add("country", country)
            person.add("position", function)
            person.add("sourceUrl", url)
            person.add("topics", "role.pep")
            emitter.emit(person)
    emitter.finalize()


def index(context, data):
    url = context.params.get("url")
    res = context.http.rehash(data)
    doc = res.html

    for link in doc.findall('.//div[@id="cosAlphaList"]//a'):
        country_url = urljoin(url, link.get("href"))
        context.log.info("Crawling country: %s (%s)", link.text, country_url)
        context.emit(data={"url": country_url, "country": link.text})
