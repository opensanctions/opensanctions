from pprint import pprint  # noqa
from datetime import datetime
from ftmstore.memorious import EntityEmitter

PSEUDO = (
    "party/unknown",
    "independent",
    "non_inscrit",
    "independant",
    "non_inscrit",
    "non-inscrit",
    "out_of_faction",
    "unknown",
    "initial-presiding-officer",
    "independiente",
    "speaker",
)


class ContextEmitter(EntityEmitter):
    def __init__(self, context, updated_at):
        self.updated_at = updated_at.isoformat()
        super(ContextEmitter, self).__init__(context)

    def emit(self, entity, rule="pass"):
        entity.context["updated_at"] = self.updated_at
        super(ContextEmitter, self).emit(entity, rule=rule)


def index(context, data):
    with context.http.rehash(data) as res:
        for country in res.json:
            for legislature in country.get("legislatures", []):
                context.emit(
                    data={
                        "country": country,
                        "legislature": legislature,
                        "url": legislature.get("popolo_url"),
                    }
                )


def parse(context, data):
    legislature = data.get("legislature")
    lastmod = int(legislature.get("lastmod"))
    lastmod = datetime.utcfromtimestamp(lastmod)
    emitter = ContextEmitter(context, lastmod)
    country = data.get("country", {}).get("code")
    entities = {}
    with context.http.rehash(data) as res:
        for person in res.json.pop("persons", []):
            parse_person(emitter, person, country, entities)

        for organization in res.json.pop("organizations", []):
            parse_organization(emitter, organization, country, entities)

        events = res.json.pop("events", [])
        events = {e.get("id"): e for e in events}

        for membership in res.json.pop("memberships", []):
            parse_membership(emitter, membership, entities, events)

        # pprint(res.json)
    emitter.finalize()


def parse_common(entity, data):
    entity.add("name", data.pop("name", None))
    entity.add("alias", data.pop("sort_name", None))
    for other in data.pop("other_names", []):
        entity.add("alias", other.get("name"))

    for link in data.pop("links", []):
        if link.get("note") in ("website", "blog", "twitter", "facebook"):
            entity.add("website", link.get("url"))
        elif "Wikipedia (" in link.get("note"):
            entity.add("wikipediaUrl", link.get("url"))
        elif "wikipedia" in link.get("note"):
            entity.add("wikipediaUrl", link.get("url"))
        # else:
        #     pprint(link)

    for ident in data.pop("identifiers", []):
        identifier = ident.get("identifier")
        scheme = ident.get("scheme")
        if scheme == "wikidata":
            entity.add("wikidataId", identifier)
        # else:
        #     pprint(ident)

    for contact_detail in data.pop("contact_details", []):
        if "email" == contact_detail.get("type"):
            entity.add("email", contact_detail.get("value"))
        if "phone" == contact_detail.get("type"):
            entity.add("phone", contact_detail.get("value"))


def parse_person(emitter, data, country, entities):
    person_id = data.pop("id", None)
    person = emitter.make("Person")
    person.make_id("EVPO", person_id)
    parse_common(person, data)
    person.add("nationality", country)

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
    person.add("email", data.pop("email", None))
    person.add("summary", data.pop("summary", None))
    person.add("topics", "role.pep")

    # data.pop("image", None)
    # data.pop("images", None)
    # if len(data):
    #     pprint(data)
    emitter.emit(person)
    entities[person_id] = person.id


def parse_organization(emitter, data, country, entities):
    org_id = data.pop("id", None)
    if org_id.lower() in PSEUDO:
        return

    classification = data.pop("classification", None)
    organization = emitter.make("Organization")
    if classification == "legislature":
        organization = emitter.make("PublicBody")
        organization.add("topics", "gov.national")
    elif classification == "party":
        organization.add("topics", "pol.party")
    else:
        emitter.log.error("Unknown org type: %s", classification)
    organization.make_id("EVPO", country, org_id)
    parse_common(organization, data)
    organization.add("legalForm", data.pop("type", None))
    organization.add("country", country)

    # data.pop("image", None)
    # data.pop("images", None)
    # data.pop("seats", None)
    # if len(data):
    #     pprint(data)
    emitter.emit(organization)
    entities[org_id] = organization.id


def parse_membership(emitter, data, entities, events):
    person_id = entities.get(data.pop("person_id", None))
    organization_id = entities.get(data.pop("organization_id", None))
    on_behalf_of_id = entities.get(data.pop("on_behalf_of_id", None))

    if person_id and organization_id:
        period_id = data.get("legislative_period_id")
        membership = emitter.make("Membership")
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
        emitter.emit(membership)

    if person_id and on_behalf_of_id:
        membership = emitter.make("Membership")
        membership.make_id(person_id, on_behalf_of_id)
        membership.add("member", person_id)
        membership.add("organization", on_behalf_of_id)

        for source in data.get("sources", []):
            membership.add("sourceUrl", source.get("url"))

        emitter.emit(membership)
