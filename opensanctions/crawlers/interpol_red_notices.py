from normality import collapse_spaces, stringify
from pprint import pformat  # noqa
from datetime import datetime
from ftmstore.memorious import EntityEmitter

from opensanctions import constants


SEXES = {
    "M": constants.MALE,
    "F": constants.FEMALE,
}

AGE_WISE_URL = "https://ws-public.interpol.int/notices/v1/red?ageMin={0}&ageMax={0}&resultPerPage=160"  # noqa
AGE_COUNTRY_WISE_URL = "https://ws-public.interpol.int/notices/v1/red?ageMin={0}&ageMax={0}&arrestWarrantCountryId={1}&resultPerPage=160"  # noqa
COUNTRY_WISE_URL = "https://ws-public.interpol.int/notices/v1/red?arrestWarrantCountryId={0}&resultPerPage=160"  # noqa


def parse_date(date):
    if date:
        try:
            date = datetime.strptime(date, "%Y/%m/%d")
        except ValueError:
            date = datetime.strptime(date, "%Y")
        return date.date()


def get_value(el):
    if el is None:
        return
    text = stringify(el.get("value"))
    if text is not None:
        return collapse_spaces(text)


def get_countries(context, data):
    with context.http.rehash(data) as result:
        doc = result.html
        wanted_by = doc.findall(
            ".//select[@id='arrestWarrantCountryId']//option"
        )  # noqa
        wanted_by = [get_value(el) for el in wanted_by]
        for country in wanted_by:
            url = COUNTRY_WISE_URL.format(country)
            data["url"] = url
            data["retry_attempt"] = 3
            data["wanted_by"] = country
            context.emit(data=data)


def parse_countrywise_noticelist(context, data):
    with context.http.rehash(data) as res:
        res = res.json
        notices = res["_embedded"]["notices"]
        for notice in notices:
            url = notice["_links"]["self"]["href"]
            if context.skip_incremental(url):
                context.emit(data={"url": url})
        total = res["total"]
        if int(total) > 160:
            for age in range(18, 100):
                url = AGE_COUNTRY_WISE_URL.format(age, data["wanted_by"])
                data["url"] = url
                context.emit(data=data, rule="fetch")


def parse_noticelist(context, data):
    with context.http.rehash(data) as res:
        res = res.json
        notices = res["_embedded"]["notices"]
        for notice in notices:
            url = notice["_links"]["self"]["href"]
            if context.skip_incremental(url):
                context.emit(data={"url": url})


def parse_notice(context, data):
    with context.http.rehash(data) as res:
        res = res.json
        first_name = res["forename"] or ""
        last_name = res["name"] or ""
        dob = res["date_of_birth"]
        nationalities = res["nationalities"]
        place_of_birth = res["place_of_birth"]
        warrants = [
            (warrant["charge"], warrant["issuing_country_id"])
            for warrant in res["arrest_warrants"]  # noqa
        ]
        gender = SEXES.get(res["sex_id"])
        emitter = EntityEmitter(context)
        entity = emitter.make("Person")
        entity.make_id("INTERPOL", first_name, last_name, res["entity_id"])
        entity.add("name", first_name + " " + last_name)
        entity.add("firstName", first_name)
        entity.add("lastName", last_name)
        entity.add("nationality", nationalities)
        for charge, country in warrants:
            entity.add("program", country)
            entity.add("summary", charge)
        entity.add("gender", gender)
        entity.add("birthPlace", place_of_birth)
        entity.add("birthDate", parse_date(dob))
        entity.add("sourceUrl", res["_links"]["self"]["href"])
        entity.add("keywords", "REDNOTICE")
        entity.add("topics", "crime")
        emitter.emit(entity)
        emitter.finalize()
