import re
import requests

from zavod import Context, helpers as h
from typing import Dict
from lxml.etree import _Element
from lxml import html

COOKIES = {
    "__arcsjs": "63473924c2c19b588d6ca3319fffc36d",
}

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Sec-Fetch-Site": "same-origin",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    # 'Accept-Encoding': 'gzip, deflate, br',
    "Sec-Fetch-Mode": "navigate",
    "Cache-Control": "max-age=0",
    "Host": "sanctionlist.mfa.ir",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Connection": "keep-alive",
    "Referer": "https://sanctionlist.mfa.ir/portal/catalogsearch/139",
    # 'Cookie': '__arcsjs=71c9c92122ecd03ad7bdac3a80a59f49; ASP.NET_SessionId=ve13dq42fjqs1srmxfd4upf4; YSP_AntiXsrfTokenKey=9bc69b92a58840039fc4f120a95a9492',
    "Sec-Fetch-Dest": "document",
}


def parse_table(
    doc: _Element,
) -> Dict[str, str]:
    return {
        item.find('.//*[@class="fg-item-title"]')
        .text_content()
        .strip(): item.find('.//*[@class="fg-item-info"]')
        .text_content()
        .strip()
        for item in doc.findall('.//*[@class="fg-item-box"]')
    }


def crawl_item(url: str, context: Context):
    response = requests.get(url, headers=HEADERS, cookies=COOKIES)

    doc = html.fromstring(response.text)

    info_dict = parse_table(doc)

    sanction_type = info_dict.pop("Sanction Type")
    schema = (
        "Person"
        if sanction_type == "Individual"
        else "Company"
        if sanction_type == "Entity"
        else "LegalEntity"
    )

    name = info_dict.pop("Name")

    # Sometimes the date has double white spaces
    sanction_date = re.sub(r"\s+", " ", info_dict.pop("Sanction Date"))
    sanction_date = h.parse_date(sanction_date, formats=["%B %d %Y"])

    sanction_title = info_dict.pop("Sanction Title")

    entity = context.make(schema)
    entity.id = context.make_id(name)

    entity.add("name", name)
    entity.add("country", info_dict.pop("Country", None) or None)
    entity.add("program", info_dict.pop("Program", None) or None)

    sanction = h.make_sanction(context, entity)
    sanction.add("date", sanction_date)
    sanction.add("description", sanction_title)

    context.emit(entity, target=True)
    context.emit(sanction)

    position_name = info_dict.pop("Position")

    if position_name != "-":
        position = h.make_position(context, position_name)
        context.emit(position)

    context.audit_data(info_dict)


def crawl(context: Context):
    page = 1

    # We are going to iterate over the pages until there is no item left
    while True:
        list_url = context.data_url + f"?page={page}&startrow={50*(page-1)}"

        response = requests.get(list_url, headers=HEADERS, cookies=COOKIES)

        doc = html.fromstring(response.text)

        doc.make_links_absolute(list_url)

        new_urls = [a.get("href") for a in doc.findall(".//tr/td/a")]

        if len(new_urls) == 0:
            break

        for url in new_urls:
            crawl_item(url, context)
