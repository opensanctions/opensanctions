from pprint import pprint  # noqa
from ftmstore.memorious import EntityEmitter
from urllib.parse import urljoin
import requests
import json

ROOT_URL = "https://memberspage.cor.europa.eu"
SEARCH_URL = "http://memberspage.cor.europa.eu/Search/Details/Person/"


def parse(context, data):
    emitter = EntityEmitter(context)
    url = data.get("url")
    with context.http.rehash(data) as res:
        doc = res.html
        divs = doc.findall('.//div[@class="regular-details"]/div')
        image_link = "{}{}".format(ROOT_URL, divs[0].find(".//img").attrib["src"])

        infos = {}
        infos["phone"] = []

        for li in divs[1].findall('.//ul[@class="no-bullet"]/li'):
            children = li.getchildren()
            title = children[0]
            if len(children) > 1:
                infos[title.text.strip()[0:-1]] = []
                for child in children:
                    if child.tag == "a":
                        infos[title.text.strip()[0:-1]].append(child.text)
                    if child.tag == "ul":
                        for li_in in child.findall("./li"):
                            infos[title.text.strip()[0:-1]].append(li_in.text)
                    if child.tag == "b":
                        for item in title.xpath(
                            "following-sibling::*/text()|following-sibling::text()"
                        ):
                            item = item.strip()
                            if item:
                                infos[title.text.strip()[0:-1]].append(item)
                    if child.tag == "img":
                        infos[title.text.strip()[0:-1]].append(
                            "image: {}{}".format(ROOT_URL, child.attrib["src"])
                        )
            elif title.tag == "b" or title.tag == "i":
                if title.tag == "i" and not title.attrib:
                    infos["description"] = title.text
                else:
                    for item in title.xpath(
                        "following-sibling::*/text()|following-sibling::text()"
                    ):
                        item = item.strip()
                        if item:
                            if title.tag == "b":
                                infos[title.text.strip()[0:-1]] = item
                            elif title.tag == "i":
                                phone_type = "phone"
                                if title.attrib["class"] == "fa fa-fax":
                                    phone_type = "fax"
                                infos["phone"].append("{}: {}".format(phone_type, item))

        first_name = infos["Full name"].split(", ")[1]
        last_name = infos["Full name"].split(", ")[0]

        if "Languages" in infos:
            infos["Languages"] = [
                info.strip() for info in infos["Languages"].split(",")
            ]

        person = emitter.make("Person")
        person.make_id(url, first_name, last_name)
        person.add("sourceUrl", url)

        person.add("firstName", first_name)
        person.add("lastName", last_name)
        person.add("name", infos.pop("Full name"))
        person.add("description", infos.get("description"))

        street = infos.get("Street", "")
        city = infos.get("City", "")
        postal_code = infos.get("Postal code", "")
        country = infos.get("Country", "")
        person.add("address", "{} {} {} {}".format(street, city, postal_code, country))
        person.add("email", email=infos.get("Emails"))
        person.add("country", infos.get("Represented Country"))
        person.add("phone", infos.get("phone"))

        # TODO: make political party into an entity
        # TODO: don't have any left-over JSON stuff :)
        infos["Photo"] = image_link
        person.add("notes", json.dumps({key: value for key, value in infos.items()}))

        emitter.emit(person)
    emitter.finalize()


def index(context, data):
    with context.http.rehash(data) as res:
        doc = res.html
        for link in doc.xpath('//div[@class="people"]/ul[@class="no-bullet"]/li'):
            person_url = urljoin(SEARCH_URL, link.attrib["id"][1::])
            person = doc.xpath(
                '//li[@id="_{}"]//ul[@class="no-bullet"]/li/a[@class="_fullname"]'.format(
                    link.attrib["id"][1::]
                )
            )[0]
            context.log.info("Crawling person: {} ({})".format(person.text, person_url))
            context.emit(data={"url": person_url})
