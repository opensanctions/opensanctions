import requests

from zavod import Context, helpers as h

# from zavod.shed.bods import parse_bods_fh
BASE_URL = "https://rpvs.gov.sk/opendatav2"
ENTITY_DETAILS_ENDPOINT = (
    f"{BASE_URL}/PartneriVerejnehoSektora/{{id}}?$expand=Partner,PravnaForma,Adresa"
)
PARTNER_DETAILS_ENDPOINT = f"{BASE_URL}/Partneri/{{id}}?$expand=Vymaz,Pokuta,OverenieIdentifikacieKUV,konecniUzivateliaVyhod,verejniFunkcionari,kvalifikovanePodnety"
# TODO: \u0161
BENEFICIAL_OWNERS_ENDPOINT = (
    f"{BASE_URL}/KonecniUzivateliaVyhod/{{id}}?$expand=Partner,PravnaForma,Adresa"
)
PUBLIC_OFFICIALS_ENDPOINT = f"{BASE_URL}/VerejniFunkcionari/{{id}}"

TOTAL_COUNT = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/$count"
FIRST_PAGE = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skip=0"
LAST_PAGE = (
    "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skiptoken=Id-261611"
)
# TODO add topics for peps and add country everywhere
# TODO check if we want 'OverenieIdentifikacieKUV'
# def crawl(context: Context) -> None:
#     fn = context.fetch_resource("source.zip", context.data_url)
#     with zipfile.ZipFile(fn, "r") as zf:
#         for name in zf.namelist():
#             if not name.endswith(".json"):
#                 continue
#             with zf.open(name, "r") as fh:
#                 parse_bods_fh(context, fh)


def check_failed_response(context, response, url):
    if response.status_code != 200:
        context.log.warn("Failed to fetch data", url=url)
        return True
    return False


def emit_address(context, entity, address):
    if not address:
        return
    street_name = address.get("MenoUlice")
    street_number = address.get("OrientacneCislo", "")
    city = address.get("Mesto")
    city_code = address.get("MestoKod", "")
    postal_code = address.get("Psc")

    address = h.make_address(
        context,
        street=f"{street_name} {street_number}".strip(),
        city=city,
        place=city_code,
        postal_code=postal_code,
    )
    h.copy_address(entity, address)


def fetch_owner_data(context, owner_id, endpoint, session):
    if not owner_id:
        return None
    owner_url = endpoint.format(id=owner_id)
    response = session.get(owner_url, headers={"Accept": "application/json"})
    if check_failed_response(context, response, owner_url):
        return None
    return response.json()


def emit_relationship(context, entity_data, entity_id, is_pep):
    last_name = entity_data.get("Priezvisko")
    dob = entity_data.get("DatumNarodenia")
    ico = entity_data.get("Ico")

    if entity_name := entity_data.get("ObchodneMeno"):
        schema = "LegalEntity"
        make_id = (ico, entity_name)
        context.log.warning("Legal entity found", entity_data=entity_data)
    elif first_name := entity_data.get("Meno"):
        schema = "Person"
        make_id = (first_name, last_name, dob)
    else:
        context.log.warn("Unknown schema", entity_data=entity_data)
        return
    # owner_id = owner_data.get("Id")

    related = context.make(schema)
    related.id = context.make_id(make_id)
    if address := entity_data.get("Adresa"):
        emit_address(context, related, address)

    if related.schema.name == "LegalEntity":
        related.add("name", entity_name)
        related.add("registrationNumber", ico)
    else:
        h.apply_name(related, first_name=first_name, last_name=last_name)
        h.apply_date(related, "birthDate", dob)
        related.add("title", entity_data.get("TitulPred"))
        related.add("title", entity_data.get("TitulZa"))
        if is_pep:
            # Based on the internal categorization provided by the source
            related.add("topics", "role.pep")
            related.add("country", "SK")
            rel = context.make("UnknownLink")
            rel.id = context.make_id(related.id, "associated with", entity_id)
            rel.add("subject", related.id)
            rel.add("object", entity_id)
            context.emit(rel)
        else:
            own = context.make("Ownership")
            own.id = context.make_id(entity_id, "owned by", related.id)
            own.add("owner", related.id)
            own.add("asset", entity_id)
            context.emit(own)
    context.emit(related)


def crawl(context: Context):
    headers = {"Accept": "application/json"}
    url = context.data_url
    url_count = 0

    while url:  # and url_count < 1:
        if url == LAST_PAGE:
            context.log.info("Stopping crawl: Reached skip token limit.")
            break
        response = requests.get(url, headers=headers)
        if check_failed_response(context, response, url):
            return

        for entry in response.json().get("value"):  # Directly iterate over new IDs
            entity_id = entry["Id"]
            context.log.info("Fetching entity details", entity_id=entity_id)
            details_url = ENTITY_DETAILS_ENDPOINT.format(id=entity_id)
            details_response = requests.get(details_url, headers=headers)

            if check_failed_response(context, details_response, details_url):
                continue

            entity_data = details_response.json()
            entity = context.make(
                "LegalEntity" if entity_data.get("ObchodneMeno") else "Person"
            )
            entity.id = context.make_id(
                entity_data.get("Id"), entity_data.get("CisloVlozky")
            )
            if entity.schema.name == "Person":
                h.apply_name(
                    entity,
                    first_name=entity_data.get("Meno"),
                    last_name=entity_data.get("Priezvisko"),
                )
                h.apply_date(entity, "birthDate", entity_data.get("DatumNarodenia"))
            else:
                entity.add("name", entity_data.get("ObchodneMeno"))
                entity.add("registrationNumber", entity_data.get("Ico"))

            if legal_form := entity_data.get("PravnaForma"):
                entity.add("legalForm", legal_form.get("Meno"))
                entity.add("classification", legal_form.get("StatistickyKod"))

            if address := entity_data.get("Adresa"):
                emit_address(context, entity, address)
            context.emit(entity)

            if partner := entity_data.get("Partner"):
                partner_id = partner.get("Id")
                partner_url = PARTNER_DETAILS_ENDPOINT.format(id=partner_id)
                partner_response = requests.get(partner_url, headers=headers)

                if check_failed_response(context, partner_response, partner_url):
                    continue

                partner_data = partner_response.json()
                # entry_number = partner_data.get("CisloVlozky")
                for owner in partner_data.get("KonecniUzivateliaVyhod"):
                    owner_data = fetch_owner_data(
                        context, owner.get("Id"), BENEFICIAL_OWNERS_ENDPOINT, requests
                    )
                    emit_relationship(context, owner_data, entity.id, is_pep=False)

                for pep in partner_data.get("VerejniFunkcionari"):
                    pep_data = fetch_owner_data(
                        context, pep.get("Id"), PUBLIC_OFFICIALS_ENDPOINT, requests
                    )
                    context.log.info("Fetched PEP data", pep_data=pep_data)
                    emit_relationship(context, pep_data, entity.id, is_pep=True)

        url = response.json().get("@odata.nextLink")
        url_count += 1  # Increment the counter
