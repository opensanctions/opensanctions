from typing import Any

from zavod import Context, helpers as h

BASE_URL = "https://rpvs.gov.sk/opendatav2"
ENTITY_DETAILS = (
    f"{BASE_URL}/PartneriVerejnehoSektora/{{id}}?$expand=Partner,PravnaForma,Adresa"
)
PARTNER_DETAILS = f"{BASE_URL}/Partneri/{{id}}?$expand=Vymaz,Pokuta,OverenieIdentifikacieKUV,konecniUzivateliaVyhod,verejniFunkcionari,kvalifikovanePodnety"
BENEFICIAL_OWNERS = (
    f"{BASE_URL}/KonecniUzivateliaVyhod/{{id}}?$expand=Partner,PravnaForma,Adresa"
)
PUBLIC_OFFICIALS = f"{BASE_URL}/VerejniFunkcionari/{{id}}"

TOTAL_COUNT = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/$count"
FIRST_PAGE = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skip=0"
IGNORE_FIELDS = [
    # @odata.context is a metadata URL
    "@odata.context",
    # Start and end dates of validity might not be the same as the dates of incorporation and dissolution
    "valid_from",
    "valid_to",
]

# TODO do we want to add contry "SK" everywhere?

# Example URLs
# https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/44?$expand=Partner,PravnaForma,Adresa
# https://rpvs.gov.sk/opendatav2/Partneri/20?$expand=Vymaz,Pokuta,OverenieIdentifikacieKUV,konecniUzivateliaVyhod,verejniFunkcionari,kvalifikovanePodnety
# https://rpvs.gov.sk/opendatav2/KonecniUzivateliaVyhod/45?$expand=Partner,PravnaForma,Adresa

# API documentation: https://rpvs.gov.sk/opendatav2/swagger/index.html
# The data source provides structured records for:
# - Legal entities and individuals registered as public sector partners
# - Their beneficial owners (koneční užívatelia výhod)
# - Public officials linked to those entities (verejní funkcionári)

# The approach here is:
# 1. Fetch a paginated list of public sector partner records.
# 2. For each record:
#    a. Retrieve expanded details about the main entity.
#    b. Fetch and emit any related beneficial owners and public officials as separate Person entities.
#    c. Emit relationships (e.g., Ownership for owners, UnknownLink for public officials) between the main
#       entity and related people.


def rename_headers(context, entry):
    result = {}
    for old_key, value in entry.items():
        new_key = context.lookup_value("columns", old_key)
        if new_key is None:
            context.log.warning("Unknown column title", column=old_key)
            new_key = old_key
        result[new_key] = value
    return result


def apply_address(context, entity, address):
    if not address:
        return
    street_name = address.pop("street_name")
    street_number = address.pop("house_number", "")
    city = address.pop("city")
    city_code = address.pop("city_code", "")
    postal_code = address.pop("postal_code")

    address = h.make_address(
        context,
        street=f"{street_name} {street_number}".strip(),
        city=city,
        place=city_code,
        postal_code=postal_code,
        lang="svk",
    )
    h.copy_address(entity, address)


def fetch_related_data(context, related_id, endpoint):
    if not related_id:
        return None
    related_url = endpoint.format(id=related_id)
    response = context.fetch_json(related_url, cache_days=3)
    return response


def emit_related_entity(context: Context, entity_data: dict[str, Any], is_pep: bool):
    first_name = entity_data.pop("name")
    last_name = entity_data.pop("surname")
    dob = entity_data.pop("dob")

    # If pulled from the list of related public officials, is_pep will be set. Beneficial owners
    # may also be public officials, in which case is_public_official will be set on the entry.
    public_official = entity_data.pop("is_public_official")

    if not first_name and not last_name:
        context.log.warn("No first or last name", entity_data=entity_data)
        return

    related = context.make("Person")
    related.id = context.make_id(first_name, last_name, dob)
    h.apply_name(related, first_name=first_name, last_name=last_name)
    h.apply_date(related, "birthDate", dob)
    related.add("title", entity_data.pop("title_prefix"))
    related.add("title", entity_data.pop("title_suffix"))
    if address := entity_data.pop("address"):
        address = rename_headers(context, address)
        apply_address(context, related, address)
    if public_official or is_pep:
        # Based on the internal categorization provided by the source
        related.add("topics", "role.pep")
        related.add("country", "SK")

    context.emit(related)
    context.audit_data(
        entity_data,
        IGNORE_FIELDS
        + [
            "id",
            # We get partner details in 'process_entry' function
            "partner",
        ],
    )
    return related


def emit_ownership(context, related, entity_id):
    own = context.make("Ownership")
    own.id = context.make_id(entity_id, "owned by", related.id)
    own.add("owner", related.id)
    own.add("asset", entity_id)
    context.emit(own)


def emit_link(context, related, entity_id):
    rel = context.make("UnknownLink")
    rel.id = context.make_id(related.id, "associated with", entity_id)
    rel.add("subject", related.id)
    rel.add("object", entity_id)
    context.emit(rel)


def process_entry(context, entry):
    entity_id = entry["Id"]
    context.log.info("Fetching entity details", entity_id=entity_id)
    details_url = ENTITY_DETAILS.format(id=entity_id)

    entity_data = context.fetch_json(details_url, cache_days=3)
    entity_data = rename_headers(context, entity_data)
    legal_entity_name = entity_data.get("trading_name", "")

    entity = context.make("LegalEntity" if legal_entity_name else "Person")
    entity.id = context.make_id(
        entity_data.pop("id"), entity_data.pop("CisloVlozky", "")
    )
    if entity.schema.name == "Person":
        h.apply_name(
            entity,
            first_name=entity_data.pop("name"),
            last_name=entity_data.pop("surname"),
        )
        h.apply_date(entity, "birthDate", entity_data.pop("dob"))
    else:
        entity.add("name", entity_data.pop("trading_name"))
        entity.add("registrationNumber", entity_data.pop("registration_number"))
        entity.add("legalForm", entity_data.pop("entity_type"))

    if legal_form := entity_data.pop("legal_form"):
        legal_form = rename_headers(context, legal_form)
        entity.add("legalForm", legal_form.pop("name"))
        entity.add("classification", legal_form.pop("economic_classification"))

    if address := entity_data.pop("address"):
        address = rename_headers(context, address)
        apply_address(context, entity, address)
    context.emit(entity)

    if partner := entity_data.pop("partner"):
        partner_id = partner.pop("Id")
        partner_url = PARTNER_DETAILS.format(id=partner_id)
        partner_data = context.fetch_json(partner_url)
        partner_data = rename_headers(context, partner_data)

        # entry_number = partner_data.pop("CisloVlozky")
        for owner in partner_data.pop("beneficial_owners"):
            owner_data = fetch_related_data(context, owner.pop("Id"), BENEFICIAL_OWNERS)
            owner_data = rename_headers(context, owner_data)
            rel_entity = emit_related_entity(context, owner_data, is_pep=False)
            emit_ownership(context, rel_entity, entity.id)

        for pep in partner_data.pop("public_officials"):
            pep_data = fetch_related_data(context, pep.pop("Id"), PUBLIC_OFFICIALS)
            pep_data = rename_headers(context, pep_data)
            rel_entity = emit_related_entity(context, pep_data, is_pep=True)
            emit_link(context, rel_entity, entity.id)

    context.audit_data(entity_data, IGNORE_FIELDS)


def crawl(context: Context):
    url = context.data_url
    total_count = context.fetch_json(TOTAL_COUNT)
    assert total_count is not None

    processed = 0
    while url and processed < total_count:
        data = context.fetch_json(url, cache_days=3)
        for entry in data.pop("value"):  # Directly iterate over new IDs
            process_entry(context, entry)
            processed += 1
        # It will break when there is no next link
        url = data.pop("@odata.nextLink")
