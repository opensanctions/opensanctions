import os
import string
from pprint import pprint  # noqa
from urllib.parse import urljoin

from opensanctions.util import jointext


class RateLimit(Exception):
    pass


API_KEY = os.environ.get("OPENSANCTIONS_COH_API_KEY")
AUTH = (API_KEY, "")
SEARCH_URL = "https://api.companieshouse.gov.uk/search/disqualified-officers"
API_URL = "https://api.companieshouse.gov.uk/disqualified-officers/natural/%s"
WEB_URL = "https://beta.companieshouse.gov.uk/register-of-disqualifications/A"


def http_get(context, url, params=None):
    res = context.http.get(url, params=params, auth=AUTH)
    if res.status_code == 429:
        raise RateLimit()
    return res


def officer(context, data):
    # emitter = EntityEmitter(context)
    officer_id = data.get("officer_id")
    url = API_URL % officer_id
    with context.http.get(url, auth=AUTH) as res:
        if res.status_code != 200:
            context.log.info("CoH error: %r", res.json)
            return
        data = res.json
        # pprint(data)
        # person = emitter.make("Person")
        person.make_slug("officer", officer_id)
        source_url = urljoin(WEB_URL, data.get("links", {}).get("self", "/"))
        person.add("sourceUrl", source_url)

        last_name = data.pop("surname", None)
        person.add("lastName", last_name)
        forename = data.pop("forename", None)
        person.add("firstName", forename)
        other_forenames = data.pop("other_forenames", None)
        person.add("middleName", other_forenames)
        person.add("name", jointext(forename, other_forenames, last_name))
        person.add("title", data.pop("title", None))

        person.add("nationality", data.pop("nationality", None))
        person.add("birthDate", data.pop("date_of_birth", None))
        person.add("topics", "crime")

        for disqual in data.pop("disqualifications", []):
            case = disqual.get("case_identifier")
            sanction = emitter.make("Sanction")
            sanction.make_id(person.id, case)
            sanction.add("entity", person)
            sanction.add("authority", "UK Companies House")
            sanction.add("program", case)
            from_date = disqual.pop("disqualified_from", None)
            person.context["created_at"] = from_date
            sanction.add("startDate", from_date)
            sanction.add("endDate", disqual.pop("disqualified_until", None))
            emitter.emit(sanction)

            address = disqual.pop("address", {})
            locality = address.get("locality")
            locality = jointext(locality, address.get("postal_code"))
            street = address.get("address_line_1")
            premises = address.get("premises")
            street = jointext(street, premises)
            address = jointext(
                street,
                address.get("address_line_2"),
                locality,
                address.get("region"),
                sep=", ",
            )
            person.add("address", address)
        emitter.emit(person, target=True)
    emitter.finalize()


def pages(context, data):
    with context.http.rehash(data) as res:
        doc = res.html
        for direct in doc.findall(".//table//a"):
            ref = direct.get("href")
            _, officer_id = ref.rsplit("/", 1)
            context.emit(data={"officer_id": officer_id})

        for a in doc.findall('.//ul[@id="pager"]/li/a'):
            next_title = a.text.strip()
            if next_title == "Next":
                url = urljoin(data.get("url"), a.get("href"))
                context.emit(rule="url", data={"url": url})


def crawl(context):
    if API_KEY is None:
        context.log.error("Please set $OPENSANCTIONS_COH_API_KEY.")
        return
    for letter in string.ascii_uppercase:
        start_index = 0
        while True:
            params = {"q": letter, "start_index": start_index, "items_per_page": 100}
            res = http_get(context, SEARCH_URL, params=params)
            if res.status_code == 416:
                break
            data = res.json()
            data.pop("items", [])
            # pprint(data)
            start_index = data["start_index"] + data["items_per_page"]
            if data["total_results"] < start_index:
                break
