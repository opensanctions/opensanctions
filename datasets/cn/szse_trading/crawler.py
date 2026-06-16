import re
from datetime import datetime, timezone, timedelta
from typing import Any

from zavod import Context, helpers as h

IGNORE = ["docpubjsonurl", "doctype", "chnlcode", "index", "navigation"]
CST = timezone(timedelta(hours=8))
NAME_DELIMITERS = ["、", "，", ","]
# The restrict_trading channel carries two notice grammars. Trading-restriction
# decisions list the parties inside the trailing （…）:
#   限制交易决定书（name1、name2（有限合伙））
RESTRICT_PREFIX = "限制交易决定书"
# Disciplinary decisions (public condemnation 公开谴责 etc.) embed the parties
# between 关于对 and 给予:
#   关于对<name1、name2>给予公开谴责处分的决定
DISCIPLINE_RE = re.compile(r"^关于对(.+?)给予.+处分的决定$")
HEADERS = {
    "Referer": "https://www.szse.cn/disclosure/supervision/transaction/restrict/index.html",
    "X-Request-Type": "ajax",
    "X-Requested-With": "XMLHttpRequest",
}


def parse_names(doc_title: str) -> list[str] | None:
    """Pull the affected parties out of an SZSE notice title.

    Returns None for any title shape we don't recognise, so the caller can skip
    it and surface the format for review rather than emit a mis-parsed name.
    Anchoring on the known prefix / regex keeps name-internal parens such as
    （有限合伙） intact instead of splitting on them.
    """
    rest = doc_title.removeprefix(RESTRICT_PREFIX)
    if rest != doc_title and rest.startswith("（") and rest.endswith("）"):
        name_part = rest[1:-1]
    else:
        match = DISCIPLINE_RE.match(doc_title)
        if match is None:
            return None
        name_part = match.group(1)
    # multi-entity notices delimit names with 、
    return h.multi_split(name_part, NAME_DELIMITERS)


def parse_item(context: Context, item: dict[str, Any]) -> None:
    # epoch millis from API; convert to ISO date for h.apply_date
    date = (
        datetime.fromtimestamp(item.pop("docpubtime") / 1000, tz=CST).date().isoformat()
    )
    doc_id = item.pop("id")
    # truncated snippet from search API; full text would require fetching docpubjsonurl
    doc_content = item.pop("doccontent")
    url = item.pop("docpuburl")

    doc_title = item.pop("doctitle")
    names = parse_names(doc_title)
    if names is None:
        context.log.warning("Skipping unrecognized notice title", title=doc_title)
        return

    for name in names:
        entity = context.make("LegalEntity")
        # doc_id is per-notice, not per-entity; name disambiguates within a notice
        entity.id = context.make_id(name, doc_id)

        entity.add("name", name)
        entity.add("country", "cn")
        entity.add("topics", "reg.action")

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "listingDate", date)
        sanction.set("sourceUrl", url)
        # notice snippet from the search API; redacted IDs in the body aren't
        # useful as identifiers, but it explains the designation.
        sanction.add("reason", doc_content)

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
        for item in result.get("data", []):
            parse_item(context, item)

        total_pages = -(-result["totalSize"] // 20)  # ceil division
        if page >= total_pages:
            break
