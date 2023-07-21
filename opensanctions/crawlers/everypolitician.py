from datetime import datetime
from urllib.parse import urljoin
from typing import Dict, Optional
from followthemoney.helpers import check_person_cutoff, post_summary


from opensanctions.core import Context
from opensanctions import helpers as h


def crawl(context: Context):
    data = context.fetch_json(context.data_url)

    for country in data:
        for legislature in country.get("legislatures", []):
            code = country.get("code").lower()
            context.log.info("Country: %s" % code)
            crawl_legislature(context, code, legislature)


def crawl_legislature(context: Context, country: str, legislature):
    lastmod_ = int(legislature.get("lastmod"))
    lastmod = datetime.utcfromtimestamp(lastmod_)

    url = urljoin(context.data_url, legislature.get("popolo"))
    # print(url)
    # this isn't being updated, hence long interval:
    data = context.fetch_json(url, cache_days=30)

    persons: Dict[str, Optional[str]] = {}
    for person in data.pop("persons", []):
        pid = person.get("id")
        persons[pid] = parse_person(context, person, country, lastmod)

    organizations: Dict[str, Optional[str]] = {}
    for org in data.pop("organizations", []):
        org_id = org.pop("id", None)
        org_id = context.lookup_value("org_id", org_id, org_id)
        if org_id is None:
            continue

        name = org.pop("name", org.pop("sort_name", None))
        organizations[org_id] = name

    events = data.pop("events", [])
    events = {e.get("id"): e for e in events}

    for membership in data.pop("memberships", []):
        parse_membership(context, membership, persons, organizations, events)


def parse_person(context: Context, data, country, lastmod):
    person_id = data.pop("id", None)
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("nationality", country)
    name = data.get("name")
    if name is None or name.lower().strip() in ("unknown",):
        return
    person.add("modifiedAt", lastmod.date())
    person.add("name", data.pop("name", None))
    person.add("alias", data.pop("sort_name", None))
    for other in data.pop("other_names", []):
        person.add("alias", other.get("name"))
    person.add("gender", data.pop("gender", None))
    person.add("title", data.pop("honorific_prefix", None))
    person.add("title", data.pop("honorific_suffix", None))
    person.add("firstName", data.pop("given_name", None))
    person.add("lastName", data.pop("family_name", None))
    person.add("fatherName", data.pop("patronymic_name", None))
    person.add("birthDate", data.pop("birth_date", None))
    person.add("deathDate", data.pop("death_date", None))
    person.add("email", h.clean_emails(data.pop("email", None)))
    person.add("notes", data.pop("summary", None))
    person.add("topics", "role.pep")

    for link in data.pop("links", []):
        url = link.get("url")
        if link.get("note") in ("website", "blog", "twitter", "facebook"):
            person.add("website", url)
        # elif "Wikipedia (" in link.get("note") and "wikipedia.org" in url:
        #     person.add("wikipediaUrl", url)
        # elif "wikipedia" in link.get("note") and "wikipedia.org" in url:
        #     person.add("wikipediaUrl", url)
        # else:
        #     person.log.info("Unknown URL", url=url, note=link.get("note"))

    for ident in data.pop("identifiers", []):
        identifier = ident.get("identifier")
        scheme = ident.get("scheme")
        if scheme == "wikidata" and identifier.startswith("Q"):
            person.add("wikidataId", identifier)

    for contact_detail in data.pop("contact_details", []):
        value = contact_detail.get("value")
        if "email" == contact_detail.get("type"):
            person.add("email", h.clean_emails(value))
        if "phone" == contact_detail.get("type"):
            person.add("phone", h.clean_phones(value))

    if check_person_cutoff(person):
        return

    # data.pop("image", None)
    # data.pop("images", None)
    # if len(data):
    #     pprint(data)
    context.emit(person, target=True)
    # entities[person_id] = person.id
    return person.id


def parse_membership(context: Context, data, persons, organizations, events):
    person_id = persons.get(data.pop("person_id", None))
    org_name = organizations.get(data.pop("organization_id", None))

    if person_id and org_name:
        period_id = data.get("legislative_period_id")
        period = events.get(period_id, {})
        comment = data.pop("role", None)
        comment = comment or period.get("name")
        starts = [data.get("start_date"), period.get("start_date")]
        ends = [data.get("end_date"), period.get("end_date")]
        # for source in data.get("sources", []):
        #     membership.add("sourceUrl", source.get("url"))

        position = post_summary(org_name, comment, starts, ends, [])
        person = context.make("Person")
        person.id = person_id
        person.add("position", position)
        context.emit(person, target=True)
