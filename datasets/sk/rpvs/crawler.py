import requests

from zavod import Context

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
    url = context.data_url
    response = requests.get(context.data_url, headers=headers)
    if response.status_code != 200:
        context.log.warn("Failed to fetch data", url=url)
        return

    # while url:
    data = response.json()
    entity_ids = [entry["Id"] for entry in data.get("value")]
    entity_ids.extend(entity_ids)
    for id in entity_ids:
        details_url = f"https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/{id}?$expand=Partner,PravnaForma,Adresa"
        details_response = requests.get(url, headers=headers)
        if details_response.status_code != 200:
            context.log.warn("Failed to fetch data", url=details_url)
            return
        entity_details = details_response.json()
        # print(entity_details)
        partner_id = entity_details.get("Partner", {}).get("Id")
        if partner_id:
            partner_url = f"https://rpvs.gov.sk/opendatav2/Partneri/{id}$expand=%20Vymaz,Pokuta,OverenieIdentifikacieKUV"
            partner_response = requests.get(partner_url, headers=headers)
            if partner_response.status_code != 200:
                context.log.warn("Failed to fetch data", url=partner_url)
                return
            partner_data = partner_response.json()
            # print(partner_data)

        # url = data.get("@odata.nextLink", None)
