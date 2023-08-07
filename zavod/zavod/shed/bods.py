import orjson
from pathlib import Path
from typing import Any, Dict, BinaryIO

from zavod.context import Context

AUDIT_IGNORE = [
    "isComponent",
    "type",
    "entityType",
    "replacesStatements",
    "statementDate",
]

SCHEME_PROPS = {
    "Not a valid Org-Id scheme, provided for backwards compatibility": "registrationNumber",  # noqa
    "DK Centrale Virksomhedsregister": "registrationNumber",
    "Danish Central Business Register": "registrationNumber",
    "AF EITI 2013-2015 beneficial ownership pilot": "alias",
    "CM EITI 2013-2015 beneficial ownership pilot": "alias",
    "GB EITI 2013-2015 beneficial ownership pilot": "alias",
    "ZM EITI 2013-2015 beneficial ownership pilot": "alias",
    "GH EITI 2013-2015 beneficial ownership pilot": "alias",
    "HN EITI 2013-2015 beneficial ownership pilot": "alias",
    "ID EITI 2013-2015 beneficial ownership pilot": "alias",
    "BF EITI 2013-2015 beneficial ownership pilot": "alias",
    "MR EITI 2013-2015 beneficial ownership pilot": "alias",
    "CD EITI 2013-2015 beneficial ownership pilot": "alias",
    "TT EITI 2013-2015 beneficial ownership pilot": "alias",
    "TG EITI 2013-2015 beneficial ownership pilot": "alias",
    "TZ EITI 2013-2015 beneficial ownership pilot": "alias",
    "LR EITI 2013-2015 beneficial ownership pilot": "alias",
    "SC EITI 2013-2015 beneficial ownership pilot": "alias",
    "NG EITI 2013-2015 beneficial ownership pilot": "alias",
    "NO EITI 2013-2015 beneficial ownership pilot": "alias",
    "MG EITI 2013-2015 beneficial ownership pilot": "alias",
    "MM EITI 2013-2015 beneficial ownership pilot": "alias",
    "ML EITI 2013-2015 beneficial ownership pilot": "alias",
    "KG EITI 2013-2015 beneficial ownership pilot": "alias",
    "EITI Structured Data - Côte d'Ivoire": "alias",
    "UA Edinyy Derzhavnyj Reestr": "registrationNumber",
    "United State Register": "registrationNumber",
    "Ministry of Justice Business Register": "registrationNumber",
    "SK Register Partnerov Verejného Sektora": "registrationNumber",
    "GB Persons Of Significant Control Register": None,
    "GB Persons Of Significant Control Register - Registration numbers": "registrationNumber",  # noqa
    "OpenOwnership Register": "sourceUrl",
    "OpenCorporates": "opencorporatesUrl",
    "Companies House": "registrationNumber",
}


def parse_statement(context: Context, data: Dict[str, Any]) -> None:
    statement_type = data.pop("statementType")
    statement_id = data.pop("statementID")
    countries = set()

    if data["isComponent"]:
        context.log.warn(f"Statement `{statement_id}` is component statement.")

    if statement_type == "personStatement":
        person_type = data.pop("personType")
        if person_type in ("unknownPerson", "anonymousPerson"):
            return

        assert person_type == "knownPerson", (person_type, data)
        proxy = context.make("Person")
        proxy.add("birthDate", data.pop("birthDate", None))
        proxy.add("deathDate", data.pop("deathDate", None))
        for name in data.pop("names", []):
            proxy.add("name", name.pop("fullName"))
            # print(name)

        for nat in data.pop("nationalities", []):
            countries.add(nat.pop("code"))
            proxy.add("nationality", nat.pop("name"))

        for country in data.pop("taxResidencies", []):
            countries.add(country.pop("code"))

        addr = data.pop("placeOfResidence", None)
        if addr is not None:
            proxy.add("address", addr.pop("address"))
            country = addr.pop("country", None)
            if country not in countries:
                countries.add(country)
                proxy.add("country", country)

    elif statement_type == "entityStatement":
        # entity_type = data.pop("entityType")  starting from v0.3.0 : public bodies
        proxy = context.make("LegalEntity")
        proxy.add("name", data.pop("name", None))

        proxy.add("alias", data.pop("alternateNames", []))
        proxy.add("incorporationDate", data.pop("foundingDate", None))
        proxy.add("dissolutionDate", data.pop("dissolutionDate", None))

        juris = data.pop("incorporatedInJurisdiction", {})
        juris_name = juris.pop("name", None)
        juris_code = juris.pop("code", juris_name)
        if len(juris):
            context.log.warn("Jurisdiction has extra data", juris=juris)
        countries.add(juris_code)
        proxy.add("jurisdiction", juris_code, original_value=juris_name)

    elif statement_type == "ownershipOrControlStatement":
        proxy = context.make("Ownership")
        interested_party = data.pop("interestedParty", {})
        proxy.add("owner", interested_party.pop("describedByPersonStatement", None))
        proxy.add("owner", interested_party.pop("describedByEntityStatement", None))
        subject = data.pop("subject", {})
        proxy.add("asset", subject.pop("describedByEntityStatement", None))
        proxy.add("date", data.pop("statementDate", None))

        for inter in data.pop("interests", []):
            proxy.add("role", inter.pop("type", None))
            proxy.add("summary", inter.pop("details", None))
            proxy.add("startDate", inter.pop("startDate", None))
            proxy.add("endDate", inter.pop("endDate", None))

    else:
        context.log.warn("Unknown statement type", statement_type)

    proxy.id = context.make_slug(statement_id)

    for addr in data.pop("addresses", []):
        proxy.add("address", addr.pop("address"))
        country = addr.pop("country", None)
        if country not in countries:
            countries.add(country)
            proxy.add("country", country)

    for ident in data.pop("identifiers", []):
        scheme = ident.pop("schemeName")
        value = ident.pop("uri", ident.pop("id", None))
        if scheme not in SCHEME_PROPS:
            context.log.warn("Unknown scheme", scheme=repr(scheme), value=value)
            continue
        if value is None:
            context.log.warn("Weird identifier", identifier=ident)
        prop = SCHEME_PROPS[scheme]
        if prop is not None:
            proxy.add(prop, value)

    # source meta for all statements, merge with `PublicationDetails`
    source = data.pop("source", {})
    proxy.add("publisher", source.pop("description", None))
    proxy.add("sourceUrl", source.pop("url", None))
    proxy.add("retrievedAt", source.pop("retrievedAt", None))

    publication = data.pop("publicationDetails", {})
    proxy.add("retrievedAt", publication.pop("publicationDate", None))
    publisher = publication.pop("publisher", {})
    proxy.add("publisher", publisher.pop("name", None))
    proxy.add("publisherUrl", publisher.pop("url", None))

    # add all the countries
    if statement_type in ("personStatement", "entityStatement"):
        proxy.add("country", countries)

    context.audit_data(data, AUDIT_IGNORE)
    context.emit(proxy)


def parse_bods_fh(context: Context, fh: BinaryIO) -> None:
    index = 0
    while line := fh.readline():
        data = orjson.loads(line)
        parse_statement(context, data)
        index += 1
        if index > 0 and index % 10000 == 0:
            context.log.info("BODS statements: %d..." % index)


def parse_bods_file(context: Context, file_name: Path):
    with open(file_name, "rb") as fh:
        parse_bods_fh(context, fh)
