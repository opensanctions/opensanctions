from datetime import datetime, timezone, timedelta

from zavod import Context, helpers as h

IGNORE = ["docpubjsonurl", "doctype", "chnlcode", "index", "navigation"]
CST = timezone(timedelta(hours=8))
HEADERS = {
    "Referer": "https://www.szse.cn/disclosure/supervision/transaction/restrict/index.html",
    "X-Request-Type": "ajax",
    "X-Requested-With": "XMLHttpRequest",
}


def parse_item(context: Context, item: dict) -> None:
    # epoch millis from API; convert to ISO date for h.apply_date
    date = (
        datetime.fromtimestamp(item.pop("docpubtime") / 1000, tz=CST).date().isoformat()
    )
    doc_id = item.pop("id")
    # truncated snippet from search API; full text would require fetching docpubjsonurl
    doc_content = item.pop("doccontent")
    url = item.pop("docpuburl")

    # title format: 限制交易决定书（name1、name2）— split on first/last parens
    doc_title = item.pop("doctitle")
    name_part = doc_title.split("（", 1)[1].rsplit("）", 1)[0]
    # multi-entity notices delimit names with 、
    names = h.multi_split(name_part, ["、", "，", ","])

    for name in names:
        entity = context.make("LegalEntity")
        # doc_id is per-notice, not per-entity; name disambiguates within a notice
        entity.id = context.make_id(name, doc_id)

        entity.add("name", name)
        # snippet as context; redacted IDs in body aren't useful as identifiers
        entity.add("notes", doc_content)
        entity.add("country", "cn")
        entity.add("topics", "reg.action")

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "listingDate", date)
        sanction.set("sourceUrl", url)

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
            headers=HEADERS,
            cache_days=1,
        )
        for item in result.get("data"):
            parse_item(context, item)

        total_pages = -(-result["totalSize"] // 20)  # ceil division
        if page >= total_pages:
            break
