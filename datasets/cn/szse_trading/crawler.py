from datetime import datetime, timezone

from zavod import Context, helpers as h

IGNORE = ["docpubjsonurl", "doctype", "chnlcode", "index", "navigation"]


def parse_item(context: Context, item: dict) -> None:
    date = (
        datetime.fromtimestamp(item.pop("docpubtime") / 1000, tz=timezone.utc)
        .date()
        .isoformat()
    )
    doc_title = item.pop("doctitle")
    doc_id = item.pop("id")
    doc_content = item.pop("doccontent")
    url = item.pop("docpuburl")
    name_part = doc_title.split("（", 1)[1].rsplit("）", 1)[0]
    names = h.multi_split(name_part, ["、", "，", ","])

    for name in names:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, doc_id)

        entity.add("name", name)
        entity.add("notes", doc_content)
        entity.add("country", "cn")
        entity.add("topics", "reg.action")
        entity.add("sourceUrl", url)

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "listingDate", date)

        context.emit(entity)
        context.emit(sanction)

    context.audit_data(item, IGNORE)


def crawl(context: Context) -> None:
    for page in range(1, 100):
        data = {
            "keyword": "",
            "time": "0",
            "range": "title",
            "channelCode[]": "restrict_trading",
            "currentPage": str(page),
            "pageSize": "20",
            "scope": "0",
        }
        result = context.fetch_json(
            context.data_url,
            data=data,
            method="POST",
            cache_days=1,
        )
        for item in result.get("data"):
            parse_item(context, item)

        total_pages = -(-result["totalSize"] // 20)  # ceil division
        if page >= total_pages:
            break
