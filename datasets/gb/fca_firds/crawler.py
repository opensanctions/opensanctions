from urllib.parse import urlencode

from zavod import Context
from zavod.shed.firds import parse_xml_file

# Total FIRDS files to fetch: 6246
MAX_RECORDS = 10_000


def get_full_dumps_index(context: Context):
    params = {
        "q": "file_type:FULINS",
        "from": int(0),
        "size": int(100),
        "pretty": "true",
    }
    total = None
    while True:
        url = f"{context.data_url}?{urlencode(params)}"
        data = context.fetch_json(url)
        if total is None:
            total = data["hits"]["total"]
            if total > MAX_RECORDS:
                context.log.warning("Total FIRDS files to fetch exceeds 10,000")
            context.log.info(f"Total FIRDS files to fetch: {total}")
        hits = data["hits"]["hits"]
        if not hits:
            break
        for hit in hits:
            src = hit["_source"]
            yield src["file_name"], src["download_link"]

        if len(hits) < params["size"]:
            break  # Last page reached
        params["from"] += params["size"]

        if params["from"] >= MAX_RECORDS:
            context.log.warning("Aborting after reaching 10,000 record cap.")
            break


def crawl(context: Context) -> None:
    for file_name, url in get_full_dumps_index(context):
        context.log.info("Fetching %s" % file_name, url=url)
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
