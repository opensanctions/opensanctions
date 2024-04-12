from typing import Any, Dict
import re
from time import sleep
from lxml import html

from zavod import Context
from zavod import helpers as h


CACHE_DAYS = 0  # since the forms are using session ids, which is unique every time
SLEEP_TIME = 1  # seconds
BASE_URL = "https://registry.andoz.tj/{section}.aspx?lang=ru"


def get_callback_pagination_params(page: int) -> str:
    """
    Returns the callback pagination params for the given page.
    Args:
        page: The page number.
    Returns:
        The callback pagination params as str.
    """
    if page > 9999:
        return f"c0:KV|2;[];GB|24;12|PAGERONCLICK7|PN{page};"
    if page > 999:
        return f"c0:KV|2;[];GB|23;12|PAGERONCLICK6|PN{page};"
    elif page > 99:
        return f"c0:KV|2;[];GB|22;12|PAGERONCLICK5|PN{page};"
    elif page > 9:
        return f"c0:KV|2;[];GB|21;12|PAGERONCLICK4|PN{page};"

    return f"c0:KV|2;[];GB|20;12|PAGERONCLICK3|PN{page};"


def fetch_form_params(context: Context, section: str) -> Dict[str, Any]:
    """
    This is a helper function to make the first request and get the form params
    for ASP.NET form.
    Args:
        context: The context object for the current dataset.
        section: Registry section (physical, foreing, legal)
    Returns:
        The form params as a dict.
    """
    params: Dict[str, str] = {}

    response = context.fetch_html(
        BASE_URL.format(section=section), cache_days=CACHE_DAYS
    )
    for s in response.findall(".//input[@type='hidden']"):
        name = s.get("name")
        value = s.get("value")
        if not name:
            continue
        params[name] = value or ""

    params["ASPxGridView1$DXSelInput"] = ""
    params["__CALLBACKID"] = "ASPxGridView1"
    params["DXScript"] = "1_44,1_76,2_34,2_41,2_33,1_69,1_67,2_36,2_27,1_54,3_7"
    params["__EVENTTARGET"] = ""
    params["__EVENTARGUMENT"] = ""
    params["ASPxTextBox1_Raw"] = "%"
    params["ASPxTextBox1"] = "%"
    params["ASPxComboBox1_VI"] = "3"
    params["ASPxComboBox1"] = "Наименование"
    params["ASPxComboBox1_DDDWS"] = "0:0:12000:792:80:0:-10000:-10000:1"
    params["ASPxComboBox1$DDD$L"] = "3"
    params["ASPxGridView1$CallbackState"] = (
        "BwIHAgIERGF0YQaIBAAAAADT5AgA0+QIAAAAAAAKAAAAAAUAAAADRUlOA0VJTgcAAAZOYW"
        "1lUnUHTmFtZSBSdQcAAAdzdWJqZWN0B3N1YmplY3QHAAAIZERhdGVSZWcKZCBEYXRlIFJl"
        "ZwcAAARzSU5OBXMgSU5OBwAAAAAAAAcABwAHAAcABv//BwIKMDIzMDAwMjA1MwcCGNCb0L"
        "jQutCy0LjQtNC40YDQvtCy0LDQvQcCG9Ka0L7QtNC40YDQvtCyINK20LDQvNGI0LXQtAcC"
        "CjE2LjAzLjUyMDAHAgkwMjUxMDM3MjUHAAcABv//BwIKNzMzMDAwMDA2OAcCGNCb0LjQut"
        "Cy0LjQtNC40YDQvtCy0LDQvQcCH9Cl0YPQtNC+0LjQtdCyINCh0LDQudGE0YPQtNC40L0H"
        "AgoyNy4wMi41MjAwBwIJNzM1MDI0Mjc3BwAHAAb//wcCCjEyMzAwMDc0NjAHAhbQlNC10L"
        "nRgdGC0LLRg9GO0YnQuNC5BwIn0KHRg9C70LDQudC80L7QvdC+0LIg0JDQsdC00YPSk9Cw"
        "0YTQvtGABwIKMDUuMDYuNTAwOQcCCTEyNTIwODMyOAcABwAG//8HAgoyMzMwMDAxNzc3Bw"
        "IY0JvQuNC60LLQuNC00LjRgNC+0LLQsNC9BwIj0KHQsNGE0LDRgNC+0LIg0JTQsNCy0LvQ"
        "sNGC0YXRg9GH0LAHAgozMS4xMi4zMjAwBwIJMjM1MDAzOTcyBwAHAAb//wcCCjEyMzAwMT"
        "EyOTIHAhjQm9C40LrQstC40LTQuNGA0L7QstCw0L0HAh/QnNGD0YXQsNC80LDQtNC40LXQ"
        "siDQn9GD0LvQsNGCBwIKMTIuMTIuMzEyOAcCCTEyNTQzMTI4MwcABwAG//8HAgo2MDMwMD"
        "I1NzUxBwIY0JvQuNC60LLQuNC00LjRgNC+0LLQsNC9BwIb0KjQvtGF0L7St9Cw0LXQsiDT"
        "rtC60YLQsNC8BwIKMDMuMDMuMzAwOQcCCTYwNTA0MTUxNwcABwAG//8HAgowNDMwMDQwNT"
        "gyBwIW0JTQtdC50YHRgtCy0YPRjtGJ0LjQuQcCH9Ch0L7Qu9C40LXQsiDQodCw0LjQtNK3"
        "0LDQvNC+0LsHAgowMS4xMC4zMDA3BwIJMDQ1MzE3MjI0BwAHAAb//wcCCjA0MzAwMjY5Nz"
        "kHAhjQm9C40LrQstC40LTQuNGA0L7QstCw0L0HAhnQkNC30LjQvNC+0LIg0JHQsNGF0YDQ"
        "vtC8BwIKMjguMDQuMjUwMAcCCTA0NTE4NjkxNAcABwAG//8HAgowNTMwMDQxNTA5BwIW0J"
        "TQtdC50YHRgtCy0YPRjtGJ0LjQuQcCGdCd0L7RgNCx0L7QtdCyINCQ0YXQvNCw0LQHAgow"
        "OC4wNS4yMjA4BwIJMDU1MDcwNzU5BwAHAAb//wcCCjAzMzAwMTAwMzgHAhjQm9C40LrQst"
        "C40LTQuNGA0L7QstCw0L0HAhvQodCw0L3Qs9C+0LLQsCDQl9GD0LvRhNC40Y8HAgoxMS4x"
        "MC4yMjA3BwIJMDM1MTIyMTM4AgVTdGF0ZQdBBwUHAAIBBwECAQcCAgEHAwIBBwQCAQcABw"
        "AHAAcABQAAAIAJAgAJAgACAAMHBAIABwACAQXT5AgABwACAQcABwA="
    )
    params["__CALLBACKPARAM"] = "c0:KV|2;[];GB|20;12|PAGERONCLICK3|PN0;"

    return params


def crawl_page(
    context: Context,
    section: str,
    params: Dict[str, str],
    page_number: int,
) -> int:
    """
    Crawls a single page of the Tadjikistan data portal and emits the data.
    Args:
        context: The context object for the current dataset.
        section: section of the registry (currently there are three)
        params: The form params to use for the request.
        page_number: The page number to crawl.
    Returns:
        The total number of pages.
    """
    entity_type = "Company"
    if section == "physical":
        entity_type = "Person"
    elif section == "foreing":
        entity_type = "LegalEntity"

    local_params = params.copy()
    local_params["__CALLBACKPARAM"] = get_callback_pagination_params(page_number)

    response = context.fetch_text(
        url=BASE_URL.format(section=section),
        method="POST",
        data=local_params,
        cache_days=CACHE_DAYS,
    )

    _, raw_payload = response.split("(", 1)
    raw_payload = raw_payload.strip("()")
    matches = re.search(r"'result':'(.*)'", raw_payload, re.M | re.S)

    if not matches:
        return 0

    dom = html.fromstring(matches.group(1))
    pages = [
        el.text
        for el in dom.xpath(".//td[contains(@class, 'dxpPageNumber_Office2003Blue')]")
    ]

    try:
        max_page = max(map(lambda x: int(x.strip("[]")), pages))
    except ValueError:
        context.log.warning(f"Cannot find max page among pages {pages}")

    for tr in dom.xpath(
        "descendant-or-self::*[@id = 'ASPxGridView1_DXMainTable']/"
        "descendant-or-self::*/tr[@class and contains(concat(' ', "
        "normalize-space(@class), ' '), ' dxgvDataRow_Office2003Blue')]"
    ):
        cells = tr.xpath("td")
        if len(cells) != 5:
            context.log.warning(
                f"Cannot parse a row {values}, skipping the rest of the page"
            )
            break

        values = [cell.text.strip() for cell in cells]
        item = dict(zip(["ein", "inn", "name", "date_of_reg", "status"], values))

        item["date_of_reg"] = h.parse_date(
            item["date_of_reg"],
            ["%d.%m.%Y"],
        )[0]

        entity = context.make(entity_type)
        entity.id = context.make_id(f"TJ{entity_type}", item["ein"], item["inn"])
        entity.add("name", item["name"], lang="tgk")
        entity.add("incorporationDate", item["date_of_reg"])
        entity.add("status", item["status"], lang="rus")
        entity.add("country", "tj")

        # I'm not sure if I'm right about mapping of this two fields, that's all I found
        # https://andoz.tj/docs/postanovleniya-pravitelstvo/Resolution__№323_ru.pdf
        # Раками Мушаххаси Андозсупоранда (РМА/INN) Идентификационный Номер Плательщика, Taxpayer Identification Number
        # Раками Ягонаи Мушаххас (РЯМ/EIN) Employer Identification number,
        # https://learn.microsoft.com/en-us/partner-center/reg-number-id
        entity.add("taxNumber", item["inn"])
        entity.add("registrationNumber", item["ein"])

        context.emit(entity, target=True)

    return max_page


def crawl(context: Context):
    """
    Main function to crawl and process data from the Unified State Register
    of the register of taxpayers.
    """

    for section in ["physical", "foreing", "legal"]:  # Yeah, foreiNG
        context.log.info(f"Processing section: {section}")

        # First we need to get the form params right
        form_params = fetch_form_params(context, section=section)

        num_pages = crawl_page(
            context=context, params=form_params, section=section, page_number=0
        )

        context.log.info(f"Total pages: {num_pages}")

        for page_number in range(1, num_pages + 1):
            sleep(SLEEP_TIME)
            crawl_page(
                context=context,
                section=section,
                params=form_params,
                page_number=page_number,
            )
