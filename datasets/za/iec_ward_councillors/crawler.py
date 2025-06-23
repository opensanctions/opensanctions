from typing import Dict

from normality import slugify

from zavod import Context
from zavod import helpers as h


def crawl_muni(context: Context, form_data: Dict[str, str], muni_btn: str) -> None:
    print(muni_btn.get("name"), muni_btn.get("value"))


    form_data["__ASYNCPOST"] = "true"
    form_data["ctl00$ctl13"] = "ctl00$MainContent$MainUpdatePanel|ctl00$MainContent$uxWardCountMunicipalListView$ctrl0$ctl00$uxMunicLinkButton"
    form_data["__VIEWSTATEGENERATOR"] = "D525B677"
    form_data["__EVENTTARGET"] = ""
    form_data["__EVENTARGUMENT"] = ""
    form_data[muni_btn.get("name")] = muni_btn.get("value")

    doc = context.fetch_html(
        context.data_url,
        method="POST",
        data=form_data,
    )

    table_xpath = './/div[@id="MainContent_uxTableCouncilorsDiv"]//table'
    table = doc.xpath(table_xpath)[0]
    headers = None
    for rownum, row in enumerate(table.findall(".//tr")):
        if headers is None:
            headers = [slugify(el.text_content(), sep="_") for el in row.findall("./th")]
            continue
        cells = [cell.text_content() for cell in row.findall("./*")]
        assert len(headers) == len(cells), (headers, cells, rownum)
        data = {hdr: cell for hdr, cell in zip(headers, cells)}
        print(data)

def crawl_province(context: Context, form_data: Dict[str, str], province_input: str) -> None:
    form_data["__EVENTTARGET"] = province_input

    doc = context.fetch_html(
        context.data_url,
        method="POST",
        data=form_data,
        #cache_days=1,
    )

    viewstate = doc.xpath('//input[@name="__VIEWSTATE"]/@value')[0]
    eventvalidation = doc.xpath('//input[@name="__EVENTVALIDATION"]/@value')[0]
    form_data = {
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
    }

    muni_btn_xpath = './/div[@id="MainContent_uxWardCountMunicipalView"]//input[@type="submit"]'
    muni_btns = doc.xpath(muni_btn_xpath)
    for muni_btn in muni_btns:
        crawl_muni(context, form_data.copy(), muni_btn)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    viewstate = doc.xpath('//input[@name="__VIEWSTATE"]/@value')[0]
    eventvalidation = doc.xpath('//input[@name="__EVENTVALIDATION"]/@value')[0]
    form_data = {
        "__VIEWSTATE": viewstate,
        "__EVENTVALIDATION": eventvalidation,
    }
    crawl_province(context, form_data.copy(), "ctl00$MainContent$btnWardCountEC")
