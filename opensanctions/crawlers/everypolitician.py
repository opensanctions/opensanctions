from datetime import datetime
from urllib.parse import unquote

from opensanctions.util import multi_split


def crawl(context):
    res = context.http.get(context.dataset.data.url)
    for country in res.json():
        for legislature in country.get("legislatures", []):
            code = country.get("code").lower()
            # if code != "bi":
            #     continue
            context.log.info("Country: %s" % code)
            crawl_legislature(context, code, legislature)


def crawl_legislature(context, country, legislature):
    lastmod = int(legislature.get("lastmod"))
    lastmod = datetime.utcfromtimestamp(lastmod)
    entities = {}

    url = legislature.get("popolo_url")
    res = context.http.get(url)
    data = res.json()

    for person in data.pop("persons", []):
        parse_person(context, person, country, entities, lastmod)

    for organization in data.pop("organizations", []):
        parse_organization(context, organization, country, entities, lastmod)

    events = data.pop("events", [])
    events = {e.get("id"): e for e in events}

    for membership in data.pop("memberships", []):
        parse_membership(context, membership, entities, events)


def parse_phones(value):
    return value


def parse_emails(value):
    emails = set()
    for email in multi_split(value, [",", "/"]):
        email = unquote(email)
        email = email.strip().strip(".")
        emails.add(email)
    return emails


def parse_common(context, entity, data, lastmod):
    entity.context["updated_at"] = lastmod.isoformat()
    entity.add("name", data.pop("name", None))
    entity.add("alias", data.pop("sort_name", None))
    for other in data.pop("other_names", []):
        entity.add("alias", other.get("name"))

    for link in data.pop("links", []):
        url = link.get("url")
        if link.get("note") in ("website", "blog", "twitter", "facebook"):
            entity.add("website", url)
        elif "Wikipedia (" in link.get("note") and "wikipedia.org" in url:
            entity.add("wikipediaUrl", url)
        elif "wikipedia" in link.get("note") and "wikipedia.org" in url:
            entity.add("wikipediaUrl", url)
        # else:
        #     context.log.info("Unknown URL", url=url, note=link.get("note"))

    for ident in data.pop("identifiers", []):
        identifier = ident.get("identifier")
        scheme = ident.get("scheme")
        if scheme == "wikidata":
            entity.add("wikidataId", identifier)
        # else:
        #     pprint(ident)

    for contact_detail in data.pop("contact_details", []):
        value = contact_detail.get("value")
        if "email" == contact_detail.get("type"):
            entity.add("email", parse_emails(value))
        if "phone" == contact_detail.get("type"):
            entity.add("phone", parse_phones(value))


def parse_person(context, data, country, entities, lastmod):
    person_id = data.pop("id", None)
    person = context.make("Person")
    person.make_slug(person_id)
    person.add("nationality", country)
    parse_common(context, person, data, lastmod)

    if data.get("birth_date", "9999") < "1900":
        return
    if data.get("death_date", "9999") < "2000":
        return

    person.add("gender", data.pop("gender", None))
    person.add("title", data.pop("honorific_prefix", None))
    person.add("title", data.pop("honorific_suffix", None))
    person.add("firstName", data.pop("given_name", None))
    person.add("lastName", data.pop("family_name", None))
    person.add("fatherName", data.pop("patronymic_name", None))
    person.add("birthDate", data.pop("birth_date", None))
    person.add("deathDate", data.pop("death_date", None))
    person.add("email", parse_emails(data.pop("email", None)))
    person.add("summary", data.pop("summary", None))
    person.add("topics", "role.pep")

    # data.pop("image", None)
    # data.pop("images", None)
    # if len(data):
    #     pprint(data)
    context.emit(person, target=True, unique=True)
    entities[person_id] = person.id


def parse_organization(context, data, country, entities, lastmod):
    org_id = data.pop("id", None)
    org_id = context.lookup_value("org_id", org_id, org_id)
    if org_id is None:
        return

    classification = data.pop("classification", None)
    organization = context.make("Organization")
    if classification == "legislature":
        organization = context.make("PublicBody")
        organization.add("topics", "gov.national")
    elif classification == "party":
        organization.add("topics", "pol.party")
    else:
        context.log.error(
            "Unknown org type",
            entity=organization,
            field="classification",
            value=classification,
        )
    organization.make_slug(country, org_id)
    if organization.id is None:
        context.log.warning(
            "No ID for organization",
            entity=organization,
            country=country,
            org_id=org_id,
        )
        return
    organization.add("country", country)
    parse_common(context, organization, data, lastmod)
    organization.add("legalForm", data.pop("type", None))

    # data.pop("image", None)
    # data.pop("images", None)
    # data.pop("seats", None)
    # if len(data):
    #     pprint(data)
    context.emit(organization)
    entities[org_id] = organization.id


def parse_membership(context, data, entities, events):
    person_id = entities.get(data.pop("person_id", None))
    organization_id = entities.get(data.pop("organization_id", None))

    if person_id and organization_id:
        period_id = data.get("legislative_period_id")
        membership = context.make("Membership")
        membership.make_id(period_id, person_id, organization_id)
        membership.add("member", person_id)
        membership.add("organization", organization_id)
        membership.add("role", data.pop("role", None))
        membership.add("startDate", data.get("start_date"))
        membership.add("endDate", data.get("end_date"))

        period = events.get(period_id, {})
        membership.add("startDate", period.get("start_date"))
        membership.add("endDate", period.get("end_date"))
        membership.add("description", period.get("name"))

        for source in data.get("sources", []):
            membership.add("sourceUrl", source.get("url"))

        # pprint(data)
        context.emit(membership)

    on_behalf_of_id = entities.get(data.pop("on_behalf_of_id", None))

    if person_id and on_behalf_of_id:
        membership = context.make("Membership")
        membership.make_id(person_id, on_behalf_of_id)
        membership.add("member", person_id)
        membership.add("organization", on_behalf_of_id)

        for source in data.get("sources", []):
            membership.add("sourceUrl", source.get("url"))

        context.emit(membership)
