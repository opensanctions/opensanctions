import time
from banal import first
from typing import Dict, List
from urllib.parse import urljoin
from requests.exceptions import RequestException
from rigour.ids import INN

from zavod import Context
from zavod import helpers as h


NO_DATES = ["Без срока погашения", "не установлена"]


def parse_date(text: str) -> str:
    if text in NO_DATES:
        return ""
    date_info = text.replace("\xa0", " ").replace("г.", "").strip().lower()
    return date_info


def crawl_item(context: Context, url: str):
    try:
        html = context.fetch_html(url, cache_days=60)
    except RequestException as re:
        time.sleep(10)
        context.log.error("HTTP error: %r" % re, url=url)
        return
    table = html.findall('.//td[@class="content"]//table')
    if len(table) != 1:
        context.log.info("ISIN announcement table not found", url=url)
        return
    values: Dict[str, Dict[str, List[str]]] = {"security": {}, "issuer": {}}
    for row in table[0].findall(".//tr"):
        if row is None:
            continue
        cells = [c.text for c in row.findall(".//td") if c.text is not None]
        if len(cells) != 2:
            continue
        key, value = [c.strip() for c in cells]
        result = context.lookup("fields", key)
        if result is None:
            context.log.warning(
                f'Unexplained field "{key}"', url=url, key=key, value=value
            )
            continue

        if result.prop is None:
            continue

        if result.prop not in values[result.entity]:
            values[result.entity][result.prop] = []

        values[result.entity][result.prop].append(value)

    isin_code = first(values["security"].get("isin", []))
    if isin_code is None:
        context.log.warn("No ISIN code on page", url=url)
        return
    security = h.make_security(context, isin_code)
    security.add("sourceUrl", url)
    security.add("topics", "sanction")
    for prop, prop_val in values["security"].items():
        if prop in ["issueDate", "maturityDate", "createdAt"]:
            parsed = [parse_date(p) for p in prop_val]
            h.apply_dates(security, prop, parsed)
        else:
            security.add(prop, prop_val)

    issuer = context.make("LegalEntity")
    inn_code = first(values["issuer"].get("innCode", []))
    is_inn_code = INN.is_valid(inn_code)
    if inn_code is not None and is_inn_code:
        issuer.id = f"ru-inn-{inn_code}"
    else:
        issuer_name = first(values["issuer"].get("name", []))
        if issuer_name is None:
            return
        issuer.id = context.make_id(isin_code, issuer_name)
    issuer.add("country", "ru")
    for prop, prop_val in values["issuer"].items():
        if prop == "innCode" and not is_inn_code:
            issuer.add("taxNumber", prop_val)
        else:
            issuer.add(prop, prop_val)
    security.add("issuer", issuer.id)
    context.emit(issuer)
    context.emit(security)


def crawl(context: Context):
    to_dt = context.data_time.strftime("%d.%m.%Y")
    for page in range(1, 901):
        context.log.info("Crawl page", page=page)
        params = {
            "keyword22": "",
            "search": "Find",
            "only_title22": "on",
            "afrom22": "01.01.2000",
            "ato22": to_dt,
            "NEWS_THEME_ID22": "",
            "form_is_submit22": 1,
            "page22": page,
        }
        try:
            doc = context.fetch_html(
                context.data_url,
                params=params,
                # cache_days=10 if page > 5 else 0,
            )
        except RequestException:
            context.log.error("Cannot fetch index page", page=page)
            time.sleep(10)
            continue
        links = 0
        for anchor in doc.findall('.//div[@class="news_sep"]//a'):
            url = urljoin(context.data_url, anchor.get("href"))
            crawl_item(context, url)
            links += 1

        if links == 0:
            break
