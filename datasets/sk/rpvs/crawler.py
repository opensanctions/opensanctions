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
    "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skiptoken=Id-261529"
)
# TODO add topics for peps and add country everywhere
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


def emit_ownership(context, owner_data, entity_id):
    owner_first_name = owner_data.get("Meno")
    owner_last_name = owner_data.get("Priezvisko")
    owner_dob = owner_data.get("DatumNarodenia")
    owner_ico = owner_data.get("Ico")
    owner_entity_name = owner_data.get("ObchodneMeno")

    if owner_entity_name and owner_ico:
        schema = "LegalEntity"
    elif owner_first_name and owner_last_name:
        schema = "Person"
    else:
        context.log.warn("Unknown schema", owner_data=owner_data)
        return
    # owner_id = owner_data.get("Id")

    owner = context.make(schema)
    owner.id = context.make_id(owner_first_name, owner_dob)
    if owner.schema.name == "Person":
        h.apply_name(owner, first_name=owner_first_name, last_name=owner_last_name)
        h.apply_date(owner, "birthDate", owner_dob)
    else:
        owner.add("name", owner_entity_name)
        owner.add("registrationNumber", owner_ico)
    address = owner_data.get("Adresa")
    if address:
        street_name = address.get("MenoUlice")
        street_number = address.get("OrientacneCislo")
        if not street_number:
            street_number = ""
        city = address.get("Mesto")
        city_code = address.get("MestoKod")
        postal_code = address.get("Psc")

        address = h.make_address(
            context,
            street=street_name + " " + street_number,
            city=city,
            place=city_code,
            postal_code=postal_code,
        )
        h.copy_address(owner, address)
    context.emit(owner)

    own = context.make("Ownership")
    own.id = context.make_id(entity_id, "owned by", owner.id)
    own.add("owner", owner.id)
    own.add("asset", entity_id)
    context.emit(own)


def emit_pep(context, pep_data):
    pep_first_name = pep_data.get("Meno")
    pep_last_name = pep_data.get("Priezvisko")
    pep_dob = pep_data.get("DatumNarodenia")

    pep = context.make("Person")
    pep.id = context.make_id(pep_first_name, pep_dob)
    h.apply_name(pep, first_name=pep_first_name, last_name=pep_last_name)
    h.apply_date(pep, "birthDate", pep_dob)
    pep.add("title", pep_data.get("TitulPred"))
    pep.add("title", pep_data.get("TitulZa"))
    # Because of the internal categorization provided by the source
    pep.add("topics", "role.pep")
    pep.add("country", "SK")
    context.emit(pep)


def crawl(context: Context):
    headers = {"Accept": "application/json"}
    # Get the initial response from context.data_url
    url = context.data_url
    url_count = 0

    while url and url_count < 3:
        response = requests.get(url, headers=headers)
        if check_failed_response(context, response, url):
            return

        data = response.json()

        for entry in data.get("value"):  # Directly iterate over new IDs
            entity_id = entry["Id"]
            context.log.info("Fetching entity details", entity_id=entity_id)
            details_url = ENTITY_DETAILS_ENDPOINT.format(id=entity_id)
            details_response = requests.get(details_url, headers=headers)
            if check_failed_response(context, details_response, details_url):
                continue
            entity_data = details_response.json()
            entity_id = entity_data.get("Id")
            entry_number = entity_data.get("CisloVlozky")

            first_name = entity_data.get("Meno")
            last_name = entity_data.get("Priezvisko")
            entity_name = entity_data.get("ObchodneMeno")
            ico = entity_data.get("Ico")
            if entity_name and ico:
                schema = "LegalEntity"
            elif first_name and last_name:
                schema = "Person"
            else:
                context.log.warn("Unknown schema", entity_data=entity_data)
                continue
            # validity_from = entity_data.get("PlatnostOd")
            # validity_to = entity_data.get("PlatnostDo")

            entity = context.make(schema)
            entity.id = context.make_id(entity_id, entry_number)
            if entity.schema.name == "Person":
                h.apply_name(entity, first_name=first_name, last_name=last_name)
                h.apply_date(entity, "birthDate", entity_data.get("DatumNarodenia"))
            else:
                entity.add("name", entity_name)
                entity.add("registrationNumber", ico)
            legal_form = entity_data.get("PravnaForma", {})
            if legal_form:
                legal_form_name = legal_form.get("Meno")
                legal_form_code = legal_form.get("StatistickyKod")
                entity.add("legalForm", legal_form_name)
                entity.add("classification", legal_form_code)

            address = entity_data.get("Adresa", {})
            if address:
                street_name = address.get("MenoUlice")
                street_number = address.get("OrientacneCislo")
                city = address.get("Mesto")
                postal_code = address.get("Psc")
                address = h.make_address(
                    context,
                    street=street_name + " " + street_number,
                    city=city,
                    postal_code=postal_code,
                )
                h.copy_address(entity, address)
            context.emit(entity)
            partner = entity_data.get("Partner", {})
            if partner:
                partner_id = partner.get("Id")
                # partner_entry_number = partner.get("CisloVlozky")
                partner_url = PARTNER_DETAILS_ENDPOINT.format(id=partner_id)
                partner_response = requests.get(partner_url, headers=headers)
                if check_failed_response(context, partner_response, partner_url):
                    continue
                partner_data = partner_response.json()
                entry_number = partner_data.get("CisloVlozky")
                # fines = partner_data.get("Pokuta", "None")
                # deletion_status = "Deleted" if partner_data.get("Vymaz") else "Active"
                for owner in partner_data.get("KonecniUzivateliaVyhod"):
                    owner_id = owner.get("Id")
                    owner_url = BENEFICIAL_OWNERS_ENDPOINT.format(id=owner_id)
                    owner_response = requests.get(owner_url, headers=headers)
                    if check_failed_response(context, owner_response, owner_url):
                        continue
                    owner_data = owner_response.json()
                    emit_ownership(context, owner_data, entity.id)

                for pep in partner_data.get("VerejniFunkcionari"):
                    pep_id = pep.get("Id")
                    pep_url = PUBLIC_OFFICIALS_ENDPOINT.format(id=pep_id)
                    pep_response = requests.get(pep_url, headers=headers)
                    if check_failed_response(context, pep_response, pep_url):
                        continue
                    pep_data = pep_response.json()
                    context.log.info("Fetched PEP data", pep_data=pep_data)
                    emit_pep(context, pep_data)

        url = data.get("@odata.nextLink")
        url_count += 1  # Increment the counter
