import requests

from zavod import Context

# import zipfile
# from zavod.shed.bods import parse_bods_fh


# def crawl(context: Context) -> None:
#     fn = context.fetch_resource("source.zip", context.data_url)
#     with zipfile.ZipFile(fn, "r") as zf:
#         for name in zf.namelist():
#             if not name.endswith(".json"):
#                 continue
#             with zf.open(name, "r") as fh:
#                 parse_bods_fh(context, fh)


def crawl(context: Context):
    endpoint = "KonecniUzivateliaVyhod"
    base_url = "https://rpvs.gov.sk/opendatav2/"
    full_url = f"{base_url}{endpoint}"

    # Filter query
    params = {
        "$skip": 0,
        "$filter": "partner/cisloVlozky eq 10",
    }

    headers = {"Accept": "application/json;odata.metadata=minimal"}

    try:
        response = requests.get(full_url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                results = response.json()
                if results:
                    for company in results.get("value", []):
                        print(company)
                else:
                    context.log.info("No results found.")
            except requests.exceptions.JSONDecodeError:
                context.log.error("Failed to decode JSON. Response: %s", response.text)
        else:
            context.log.error(
                "Failed to retrieve data. Status code: %s. Response: %s",
                response.status_code,
                response.text,
            )

    except requests.exceptions.RequestException as e:
        context.log.error("Request failed: %s", str(e))
