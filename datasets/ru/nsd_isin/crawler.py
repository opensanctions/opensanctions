import time
from banal import first
from datetime import datetime
from urllib.parse import urljoin
from requests.exceptions import RequestException

from zavod import Context

MONTHS = {
    "сентября": "Sep",
    "августа": "Aug",
    "ноября": "Nov",
    "октября": "Oct",
    "июля": "Jul",
    "марта": "Mar",
    "апреля": "Apr",
    "июня": "Jun",
    "декабря": "Dec",
    "мая": "May",
    "февраля": "Feb",
    "января": "Jan",
}

NO_DATES = ["Без срока погашения", "не установлена"]


def parse_date(text: str) -> str:
    for ru, en in MONTHS.items():
        text = text.replace(ru, en)
    if text in NO_DATES:
        return ""
    text = text.replace("\xa0", " ").replace("г.", "").strip()
    dt = datetime.strptime(text, "%d %b %Y")
    return dt.date().isoformat()


def crawl_item(context: Context, url: str):
    try:
        html = context.fetch_html(url, cache_days=30)
    except RequestException as re:
        time.sleep(10)
        context.log.error("HTTP error: %r" % re, url=url)
        return
    table = html.findall('.//td[@class="content"]//table')
    if len(table) != 1:
        context.log.info("ISIN announcement table not found", url=url)
        return
    values = {"security": {}, "issuer": {}}
    for row in table[0].findall(".//tr"):
        if row is None:
            continue
        cells = [c.text for c in row.findall(".//td") if c.text is not None]
        if len(cells) != 2:
            continue
        key, value = [c.strip() for c in cells]
        result = context.lookup("fields", key)
        if result is None:
            context.log.warning("Unexplained field", url=url, key=key, value=value)
            continue

        if result.prop is None:
            continue

        if result.type == "date":
            try:
                value = parse_date(value)
            except ValueError:
                context.log.warning("Cannot parse date", key=key, value=value)
                continue

        if result.prop not in values[result.entity]:
            values[result.entity][result.prop] = []

        values[result.entity][result.prop].append(value)

    isin_code = first(values["security"].get("isin"))
    if isin_code is None:
        context.log.warn("No ISIN code on page", url=url)
        return
    security = context.make("Security")
    security.id = context.make_slug(isin_code)
    security.add("sourceUrl", url)
    security.add("country", "ru")
    for prop, value in values["security"].items():
        security.add(prop, value)
    context.emit(security, target=True)

    issuer = context.make("LegalEntity")
    inn_code = first(values["issuer"].get("innCode"))
    if inn_code is not None:
        issuer.id = f"ru-inn-{inn_code}"
    else:
        issuer_name = first(values["issuer"].get("name"))
        if issuer_name is None:
            return
        issuer.id = context.make_id(isin_code, issuer_name)
    issuer.add("country", "ru")
    for prop, value in values["issuer"].items():
        issuer.add(prop, value)
    context.emit(issuer)


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
            doc = context.fetch_html(context.data_url, params=params)
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
