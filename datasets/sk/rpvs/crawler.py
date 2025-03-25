import requests

from zavod import Context, helpers as h

# from zavod.shed.bods import parse_bods_fh
BASE_URL = "https://rpvs.gov.sk/opendatav2"
ENTITY_DETAILS_ENDPOINT = (
    f"{BASE_URL}/PartneriVerejnehoSektora/{{id}}?$expand=Partner,PravnaForma,Adresa"
)
PARTNER_DETAILS_ENDPOINT = f"{BASE_URL}/Partneri/{{id}}?$expand=Vymaz,Pokuta,OverenieIdentifikacieKUV,konecniUzivateliaVyhod,verejniFunkcionari,kvalifikovanePodnety"


TOTAL_COUNT = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/$count"
FIRST_PAGE = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skip=0"
LAST_PAGE = (
    "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skiptoken=Id-261529"
)

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


def crawl(context: Context):
    entity_ids = []
    headers = {"Accept": "application/json"}
    response = requests.get(context.data_url, headers=headers)
    if check_failed_response(context, response, context.data_url):
        return

    # while url:
    data = response.json()
    entity_ids = [entry["Id"] for entry in data.get("value")]
    for id in entity_ids:
        details_url = ENTITY_DETAILS_ENDPOINT.format(id=id)
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
            h.apply_name(
                entity,
                first_name=first_name,
                last_name=last_name,
            )
            h.apply_date(entity, "birthDate", entity_data.get("DatumNarodenia"))
        else:
            entity.add("name", entity_name)
            entity.add("idNumber", ico)
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

            # Extract beneficial owners
            beneficial_owners = []
            for owner in partner_data.get("KonecniUzivateliaVyhod", []):
                owner_name = f"{owner.get('Meno')} {owner.get('Priezvisko')}"
                owner_dob = owner.get("DatumNarodenia")
                owner_id = owner.get("Id")
                beneficial_owners.append((owner_name, owner_dob))
                owner = context.make("Person")
                owner.id = context.make_id(owner_name, owner_dob, owner_id)
                h.apply_name(owner, owner_name)
                h.apply_date(owner, "birthDate", owner_dob)
                context.emit(owner)

                own = context.make("Ownership")
                own.id = context.make_id(entity.id, "owned by", owner.id)
                own.add("owner", owner.id)
                own.add("asset", entity.id)
                context.emit(own)

            # # Extract verification details
            # verification_status = "Not Verified"
            # verification_data = partner_data.get("OverenieIdentifikacieKUV")
            # if verification_data:
            #     verification_status = (
            #         f"Verified on {verification_data.get('DatumOverenia')}"
            #     )

            # # Extract public officials
            # public_officials = [
            #     f"{f.get('Meno')} {f.get('Priezvisko')}"
            #     for f in partner_data.get("VerejniFunkcionari", [])
            # ]
            # print(public_officials)

        # url = data.get("@odata.nextLink", None)
