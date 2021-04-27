import string
from lxml import html
from pprint import pprint  # noqa
from urllib.parse import urljoin, urlencode


def crawl_entry(context, entry):
    link = entry.find(".//a")
    url = context.dataset.data.url
    url = urljoin(url, link.get("href"))
    _, member_id = url.rsplit("=", 1)
    person = context.make("Person")
    person.make_slug(member_id)
    person.add("name", link.text)
    person.add("sourceUrl", url)
    last_name, first_name = link.text.split(", ", 1)
    person.add("lastName", last_name)
    person.add("firstName", first_name)
    person.add("position", entry.findtext('.//span[@class="fonction"]'))
    role, country = entry.findall('.//span[@class="infos"]')
    person.add("summary", role.text_content().strip())
    person.add("nationality", country.text_content())
    person.add("topics", "role.pep")
    context.emit(person, target=True)


def crawl(context):
    seen = set()
    data_url = context.dataset.data.url
    for letter in string.ascii_uppercase:
        params = {"initial": letter, "offset": 0}
        url = data_url + "?" + urlencode(params)
        while url is not None:
            res = context.http.get(url)
            doc = html.fromstring(res.text)
            for member in doc.findall('.//ul[@class="member-results"]/li'):
                crawl_entry(context, member)

            seen.add(url)
            url = None
            for a in doc.findall('.//div[@id="pagination"]//a'):
                next_url = urljoin(data_url, a.get("href"))
                if next_url not in seen:
                    url = next_url
