import requests

from zavod import Context, helpers as h

# import zipfile
# from zavod.shed.bods import parse_bods_fh


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


def crawl(context: Context):
    entity_ids = []
    headers = {"Accept": "application/json"}
    # url = context.data_url
    response = requests.get(context.data_url, headers=headers)
    if response.status_code != 200:
        context.log.warn("Failed to fetch data", url=context.data_url)
        return

    # while url:
    data = response.json()
    entity_ids = [entry["Id"] for entry in data.get("value")]
    for id in entity_ids:
        details_url = f"https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/{id}?$expand=Partner,PravnaForma,Adresa"
        details_response = requests.get(details_url, headers=headers)
        if details_response.status_code != 200:
            context.log.warn("Failed to fetch data", url=details_url)
            return
        # context.log.info(
        #     "Fetched data", url=details_url, status=details_response.status_code
        # )
        entity_data = details_response.json()
        entity_id = entity_data.get("Id")
        entry_number = entity_data.get("CisloVlozky")

        first_name = entity_data.get("Meno")
        last_name = entity_data.get("Priezvisko")
        # birth_date = entity_data.get("DatumNarodenia")
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
            h.make_address(
                context,
                street=street_name,
                street2=street_number,
                city=city,
                postal_code=postal_code,
            )
        context.emit(entity)
        partner = entity_data.get("Partner", {})
        if partner:
            partner_id = partner.get("Id")
            print(partner_id)
            partner_entry_number = partner.get("CisloVlozky")

        # if partner_id:
        #     partner_url = f"https://rpvs.gov.sk/opendatav2/Partneri/{id}?$expand=Vymaz,Pokuta,OverenieIdentifikacieKUV,konecniUzivateliaVyhod,verejniFunkcionari,kvalifikovanePodnety"
        #     partner_response = requests.get(partner_url, headers=headers)
        #     if partner_response.status_code != 200:
        #         context.log.warn("Failed to fetch data", url=partner_url)
        #         return
        #     partner_data = partner_response.json()
        #     entry_number = partner_data.get("CisloVlozky")
        #     fines = partner_data.get("Pokuta", "None")
        #     deletion_status = "Deleted" if partner_data.get("Vymaz") else "Active"

        #     # Extract beneficial owners
        #     beneficial_owners = []
        #     for owner in partner_data.get("KonecniUzivateliaVyhod", []):
        #         owner_name = f"{owner.get('Meno')} {owner.get('Priezvisko')}"
        #         owner_dob = owner.get("DatumNarodenia")
        #         beneficial_owners.append((owner_name, owner_dob))

        #     # Extract verification details
        #     verification_status = "Not Verified"
        #     verification_data = partner_data.get("OverenieIdentifikacieKUV")
        #     if verification_data:
        #         verification_status = (
        #             f"Verified on {verification_data.get('DatumOverenia')}"
        #         )

        #     # Extract public officials
        #     public_officials = [
        #         f"{f.get('Meno')} {f.get('Priezvisko')}"
        #         for f in partner_data.get("VerejniFunkcionari", [])
        #     ]
        #     print(public_officials)

        # url = data.get("@odata.nextLink", None)
