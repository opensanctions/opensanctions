from normality import collapse_spaces, stringify
from pprint import pformat  # noqa
from datetime import datetime
from ftmstore.memorious import EntityEmitter

from opensanctions import constants


SEXES = {
    "M": constants.MALE,
    "F": constants.FEMALE,
}

AGE_WISE_URL = "https://ws-public.interpol.int/notices/v1/yellow?ageMin={0}&ageMax={0}&resultPerPage=160"  # noqa
AGE_NATIONALITY_WISE_URL = "https://ws-public.interpol.int/notices/v1/yellow?ageMin={0}&ageMax={0}&nationality={1}&resultPerPage=160"  # noqa
NATIONALITY_WISE_URL = "https://ws-public.interpol.int/notices/v1/yellow?nationality={0}&resultPerPage=160"  # noqa


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


def get_nationalities(context, data):
    with context.http.rehash(data) as result:
        doc = result.html
        nationalities = doc.findall(".//select[@id='nationality']//option")
        nationalities = [get_value(el) for el in nationalities]
        for nationality in nationalities:
            url = NATIONALITY_WISE_URL.format(nationality)
            data["url"] = url
            data["retry_attempt"] = 3
            data["nationality"] = nationality
            context.emit(data=data)


def parse_nationalitywise_noticelist(context, data):
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
                url = AGE_NATIONALITY_WISE_URL.format(age, data["nationality"])
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
        gender = SEXES.get(res["sex_id"])
        emitter = EntityEmitter(context)
        entity = emitter.make("Person")
        entity.make_id("INTERPOL", first_name, last_name, res["entity_id"])
        entity.add("name", first_name + " " + last_name)
        entity.add("firstName", first_name)
        entity.add("lastName", last_name)
        entity.add("nationality", nationalities)
        entity.add("gender", gender)
        entity.add("birthPlace", place_of_birth)
        entity.add("birthDate", parse_date(dob))
        entity.add("sourceUrl", res["_links"]["self"]["href"])
        entity.add("keywords", "YELLOWNOTICE")
        entity.add("topics", "researched")
        emitter.emit(entity)
        emitter.finalize()
