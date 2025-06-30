from urllib.parse import urlencode
from datetime import datetime, timedelta

from zavod import Context
from zavod.shed.firds import latest_full_set, parse_xml_file


# https://api.data.fca.org.uk/fca_data_firds_files?q=((file_type:FULINS)%20AND%20(publication_date:[2017-10-15%20TO%202017-12-31]))&from=0&size=100&pretty=true
def get_recent_full_dump_urls(context: Context):
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


def crawl(context: Context) -> None:
    for file_name, url in latest_full_set(context, get_recent_full_dump_urls(context)):
        context.log.info("Fetching %s" % file_name, url=url)
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
