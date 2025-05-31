from zavod import Context
from zavod.shed.firds import parse_xml_file


def get_full_dumps_index(context: Context):
    page = 0
    size = 100
    total = None
    while True:
        url = f"{context.data_url}?q=file_type:FULINS&from={page * size}&size={size}&pretty=true"
        data = context.fetch_json(url)
        if total is None:
            total = data["hits"]["total"]
            context.log.info(f"Total FIRDS files to fetch: {total}")
        hits = data["hits"]["hits"]
        if not hits:
            break
        for hit in hits:
            src = hit["_source"]
            yield src["file_name"], src["download_link"]
        page += 1
        if page * size >= total:
            break


def crawl(context: Context) -> None:
    for file_name, url in get_full_dumps_index(context):
        context.log.info("Fetching %s" % file_name, url=url)
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
