import time
from datetime import datetime
from urllib.parse import urljoin
from requests.exceptions import RequestException

from opensanctions import settings
from opensanctions.core import Context

# . SEARCH_URL =

IGNORE_FIELDS = [
    "Управляющая компания",
    "Специализированный депозитарий",
    "Реестродержатель",
    "Регистратор",
    "Управляющий ипотечным покрытием",  # Mortgage coverage manager
    "Порядок хранения/учета",
    "Дата принятия решения",
    "Финансовый инструмент",
    "Коэффициент (ДР : Представляемые ценные бумаги)",
    "Номер решения о формировании имущественного пула",  # Number of the decision on the formation of the property pool
    "Дата государственной регистрации правил фонда",  # Date of state registration of fund rules
]

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
        time.sleep(2)
        context.log.error("HTTP error: %r" % re, url=url)
        return
    table = html.findall('.//td[@class="content"]//table')
    if len(table) != 1:
        context.log.info("ISIN announcement table not found", url=url)
        return
    security = context.make("Security")
    security.add("sourceUrl", url)
    security.add("country", "ru")
    issuer = context.make("LegalEntity")
    issuer.add("country", "ru")
    isin_code = None
    for row in table[0].findall(".//tr"):
        if row is None:
            continue
        cells = [c.text for c in row.findall(".//td") if c.text is not None]
        if len(cells) != 2:
            continue
        key, value = [c.strip() for c in cells]
        if key == "ISIN код":
            security.id = context.make_slug(value)
            security.add("isin", value)
            isin_code = value
        elif key in (
            "Дата присвоения кода",
            "Дата присвоения идентификационного номера выпуска",
        ):
            # ISIN assignment date, discard
            security.add("createdAt", parse_date(value))
        elif key in (
            "Эмитент",
            "Полное наименование организации, выдающей КСУ",
            "Наименование фонда",  # "Fund Name"
        ):
            issuer.add("name", value)
            issuer.id = context.make_id(isin_code, value)
        elif key == "ИНН эмитента":
            issuer.add("innCode", value)
            issuer.id = f"ru-inn-{value}"
        elif key in (
            "Полное наименование индекса на английском языке",  # Full name of the index in English
            "Полное наименование индекса",  # Full name of the index
            "Наименование выпуска/транша",
            "Полное наименование финансового инструмента",
            "Наименование выпуска/транша",
            "Наименование имущественного пула",
            "Наименование ипотечного сертификата участия с ипотечным покрытием",  # Name of the mortgage participation certificate with mortgage coverage
            "Наименование ипотечных сертификатов участия с ипотечным покрытием",  # Name of mortgage participation certificates with mortgage coverage
            "Наименование ипотечных сертификатов участия",  # Name of mortgage participation certificates
            "Полное наименование депозитной ставки",  # Full name of the deposit rate
            "Полное наименование инструмента",  # Full name of the tool
            "Наименование финансового инструмента",  # Name of the financial instrument
        ):
            security.add("name", value)
        elif key == "Тип фонда":
            issuer.add("legalForm", value)
        elif key in (
            "Номинал",
            "Номинальная стоимость каждой ценной бумаги",
        ):
            security.add("amount", value)
        elif key == "Валюта номинала":
            security.add("currency", value)
        elif key == "Форма выпуска ценной бумаги":
            security.add("classification", value)
        elif key in (
            "Краткое наименование депозитной ставки",  # Short name of the deposit rate
            "Краткое наименование индекса",  # Short name of the index
            "Краткое наименование индекса на английском языке",  # Short name of the index in English
            "Краткое наименование инструмента",  # Short name of the tool
            "Краткое наименование финансового инструмента",  # Short name of the financial instrument
        ):
            security.add("ticker", value)
        elif key in (
            "Pегистрационный номер",
            "Регистрационный номер",
            "Идентификационный номер выпуска",
            "Регистрационный номер выпуска",  # Issue registration number
            "Государственный регистрационный номер выпуска",  # State registration number of the issue
            "Государственный регистрационный номер правил Д.У.",  # State registration number of the rules D.U.
            "Государственный регистрационный номер правил фонда",  # State registration number of the rules of the fund
            "Государственный регистрационный номер правил",  # State registration number of the rules
        ):
            security.add("registrationNumber", value)
        elif key in ("Дата погашения",):
            security.add("maturityDate", parse_date(value))
        elif key in (
            "Дата регистрации",
            "Дата допуска к торгам на фондовой бирже в процессе размещения",
            "Дата государственной регистрации выпуска",  # 'Date of state registration of the issue'
            "Дата допуска к торгам на фондовой бирже в процессе размещения",  # 'Date of admission to trading on the stock exchange in the process of placement'
            "Дата регистрации выпуска",  # Issue registration date
            "Дата государственной регистрации правил Д.У.",  # Date of state registration of D.U.
            "Дата государственной регистрации правил",  # Date of state registration of the rules
        ):
            try:
                security.add("issueDate", parse_date(value))
            except ValueError:
                context.log.warning("Cannot parse date", key=key, value=value)
        elif key in (
            "Вид, категория ценной бумаги",
            "Вид, категория, тип ценной бумаги",
            "Тип ценной бумаги",
        ):
            security.add("type", value)
        elif key in IGNORE_FIELDS:
            pass
        else:
            context.log.warning("Unexplained field", url=url, key=key, value=value)
            # context.inspect(cells)

    if issuer.id is not None:
        security.add("issuer", issuer)
        context.emit(issuer)
    if security.id is not None:
        context.emit(security, target=True)
    else:
        context.log.warn(
            "Security has no ISIN",
            security=security,
            isin=isin_code,
            url=url,
        )


def crawl(context: Context):
    to_dt = settings.RUN_TIME.strftime("%d.%m.%Y")
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
            doc = context.fetch_html(context.source.data.url, params=params)
        except RequestException:
            context.log.error("Cannot fetch index page", page=page)
            time.sleep(2)
            continue
        links = 0
        for anchor in doc.findall('.//div[@class="news_sep"]//a'):
            url = urljoin(context.source.data.url, anchor.get("href"))
            crawl_item(context, url)
            links += 1

        if links == 0:
            break
