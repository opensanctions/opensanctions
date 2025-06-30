from datetime import datetime, timedelta
from zavod import Context
from zavod.shed.firds import parse_xml_file, latest_full_set


def get_recent_full_dump_urls(context: Context):
    from_date = (datetime.now() - timedelta(days=30)).isoformat()[:10]
    to_date = datetime.now().isoformat()[:10]
    query = {
        "fq": f"publication_date:[{from_date} TO {to_date}]",
        "core": "esma_registers_firds_files",
        "pagingSize": "100",
        "start": 0,
        "keyword": "",
        "sortField": "publication_date desc",
        "criteria": [
            {
                "name": "file_type",
                "value": "file_type:FULINS",
                "type": "custom1",
                "isParent": True,
            },
        ],
        "wt": "json",
    }
    resp = context.http.post(context.data_url, json=query)
    resp_data = resp.json()
    latest = None
    for result in resp_data["response"]["docs"]:
        if latest is not None and latest != result["publication_date"]:
            break
        latest = result["publication_date"]
        yield result["file_name"], result["download_link"]


def crawl(context: Context) -> None:
    for file_name, url in latest_full_set(context, get_recent_full_dump_urls(context)):
        context.log.info("Fetching %s" % file_name, url=url)
        path = context.fetch_resource(file_name, url)
        parse_xml_file(context, path)
