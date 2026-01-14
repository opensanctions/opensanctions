import re
from datetime import datetime
from urllib.parse import urljoin, unquote
from typing import Dict, Optional, Set
from followthemoney.helpers import post_summary

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


PHONE_SPLITS = [",", "/", "(1)", "(2)", "(3)", "(4)", "(5)", "(6)", "(7)", "(8)"]
PHONE_REMOVE = re.compile(r"(ex|ext|extension|fax|tel|\:|\-)", re.IGNORECASE)


def clean_emails(emails):
    out = []
    for email in h.multi_split(emails, ["/", ","]):
        if email is None:
            return
        email = unquote(email)
        email = email.strip()
        email = email.rstrip(".")
        out.append(email)
    return out


def clean_phones(phones):
    out = []
    for phone in h.multi_split(phones, PHONE_SPLITS):
        phone = PHONE_REMOVE.sub("", phone)
        out.append(phone)
    return out


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

    birth_dates: Dict[str, str] = {}
    death_dates: Dict[str, str] = {}
    for person in data.get("persons"):
        death_date = person.get("death_date", None)
        if death_date is not None:
            death_dates[person.get("id")] = death_date
        birth_date = person.get("birth_date", None)
        if birth_date is not None:
            birth_dates[person.get("id")] = birth_date

    peps: Set[str] = set()
    for membership in data.pop("memberships"):
        person_id = parse_membership(
            context,
            country,
            membership,
            organizations,
            events,
            birth_dates,
            death_dates,
        )
        if person_id is not None:
            peps.add(person_id)

    for person in data.get("persons"):
        if person.get("id") in peps:
            parse_person(context, person, country, lastmod)


def person_entity_id(context, person_id: str) -> str:
    return context.make_slug(person_id)


def parse_person(context: Context, data, country, lastmod) -> None:
    person_id = data.pop("id", None)
    person = context.make("Person")
    person.id = person_entity_id(context, person_id)
    person.add("nationality", country)
    name = data.get("name")
    if name is None or name.lower().strip() in ("unknown",):
        return
    person.add("modifiedAt", lastmod.date())
    person.add("name", data.pop("name", None))
    person.add("alias", data.pop("sort_name", None))
    for other in data.pop("other_names", []):
        lang = other.get("lang")
        person.add("alias", other.get("name"), lang=lang)
    # person.add("gender", data.pop("gender", None))
    person.add("title", data.pop("honorific_prefix", None))
    person.add("title", data.pop("honorific_suffix", None))
    person.add("firstName", data.pop("given_name", None))
    person.add("lastName", data.pop("family_name", None))
    person.add("fatherName", data.pop("patronymic_name", None))
    person.add("birthDate", data.pop("birth_date", None))
    person.add("deathDate", data.pop("death_date", None))
    person.add("email", clean_emails(data.pop("email", None)))
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
            person.add("email", clean_emails(value))
        if "phone" == contact_detail.get("type"):
            person.add("phone", clean_phones(value))

    # data.pop("image", None)
    # data.pop("images", None)
    # if len(data):
    #     pprint(data)
    context.emit(person)


def parse_membership(
    context: Context,
    country,
    data,
    organizations,
    events,
    birth_dates: Dict[str, str],
    death_dates: Dict[str, str],
) -> Optional[str]:
    person_id = data.pop("person_id", None)
    org_name = organizations.get(data.pop("organization_id", None))

    if person_id and org_name:
        period_id = data.get("legislative_period_id")
        period = events.get(period_id, {})
        role = data.pop("role", None)
        role = role or period.get("name")
        if role != "member":
            context.log.warning("Unexpected role", role=role)

        starts = [data.get("start_date"), period.get("start_date")]
        ends = [data.get("end_date"), period.get("end_date")]
        # for source in data.get("sources", []):
        #     membership.add("sourceUrl", source.get("url"))

        position_label = f"{role.title()} of the {org_name}"
        res = context.lookup("position_label", position_label)
        if res:
            position_label = res.value

        position = h.make_position(
            context,
            position_label,
            country=country,
            topics=["gov.national", "gov.legislative"],
        )
        categorisation = categorise(context, position, True)

        position_property = post_summary(org_name, role, starts, ends, [])
        person = context.make("Person")
        person.id = person_entity_id(context, person_id)
        person.add("position", position_property)

        if categorisation.is_pep:
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                no_end_implies_current=False,
                start_date=data.get("start_date") or period.get("start_date"),
                end_date=data.get("end_date") or period.get("end_date"),
                birth_date=birth_dates.get(person_id, None),
                death_date=death_dates.get(
                    person_id,
                    None,
                ),
                categorisation=categorisation,
            )
            if occupancy:
                context.emit(position)
                context.emit(occupancy)
                context.emit(person)
                return person_id
