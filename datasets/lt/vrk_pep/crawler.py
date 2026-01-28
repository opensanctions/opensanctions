from zavod import Context, helpers as h
from zavod.extract import zyte_api


HEADERS = {
    "referer": "https://atviriduomenys.vrk.lt/datasets/gov/vrk/Isrinkti",
    "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (zavod; opensanctions.org)",
    "accept": "application/json",
}

def crawl(context: Context) -> None: 
    # data = zyte_api.fetch_json(
    #     context, 
    #     context.data_url#,
    #     #cache_days=1
    # )
    data = context.fetch_json(context.data_url, headers=HEADERS)
    breakpoint()


# import requests
# BASE = "https://get.data.gov.lt/datasets/gov/vrk/isrinkti/Isrinktas"
# r = requests.get(BASE, timeout=60)
# r.raise_for_status()
# data = r.json()
# also check datasets avail: curl -sS "https://get.data.gov.lt/datasets/gov/vrk/:ns"
# breakpoint()