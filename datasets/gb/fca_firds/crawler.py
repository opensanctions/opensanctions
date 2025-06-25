from collections import defaultdict
from urllib.parse import urlencode
import re
from datetime import datetime, timedelta

from zavod import Context
from zavod.shed.firds import parse_xml_file

# Total FIRDS files to fetch: 6246
MAX_RECORDS = 10_000
REGEX_DATE = re.compile(r"_(20\d{6})_")


# https://api.data.fca.org.uk/fca_data_firds_files?q=((file_type:FULINS)%20AND%20(publication_date:[2017-10-15%20TO%202017-12-31]))&from=0&size=100&pretty=true
def get_recent_full_data_files(context: Context):
    from_date = (datetime.now() - timedelta(days=30)).isoformat()[:10]
    to_date = datetime.now().isoformat()[:10]
    params = {
        "q": f"file_type:FULINS AND publication_date:[{from_date} TO {to_date}]",
        "from": 0,
        "size": 100,
        "pretty": "true",
        "sort": "file_name:asc",
    }
    total = None
    while total is None or params["from"] <= total:
        url = f"{context.data_url}?{urlencode(params)}"
        data = context.fetch_json(url)
        total = data["hits"]["total"]
        for hit in data["hits"]["hits"]:
            src = hit["_source"]
            yield src["file_name"], src["download_link"]

        params["from"] += params["size"]


def get_latest_full_set(context: Context):
    date_sets = defaultdict(list)
    for file_name, url in get_recent_full_data_files(context):
        match = REGEX_DATE.search(url)
        if not match:
            context.log.warning(f"URL {url} does not match expected date format.")
            continue
        date_str = match.group(1)
        date_sets[date_str].append((file_name, url))
    latest = max(date_sets.keys())
    return date_sets[latest]


def crawl(context: Context) -> None:
    for file_name, url in get_latest_full_set(context):
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
