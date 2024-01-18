from typing import Optional
from xml.etree import ElementTree
from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise
from normality import collapse_spaces


def crawl_member_bio(context, url):
    print(f"Processing  {url}")
    doc = context.fetch_html(url, cache_days=1)

    name = collapse_spaces(
        doc.xpath('//div[@class="sn_narys_vardas_title"]')[0].text_content()
    )

    date_of_birth = (
        collapse_spaces(
            doc.xpath(
                '//tr[.//*[contains(.//text(), "Date of birth")]]//td//p|//p[.//*[contains(.//text(), "Date of birth")]]'
            )[-1].text_content()
        )
        .replace("Date of birth", "")
        .strip()
    )
    place_of_birth = (
        collapse_spaces(
            doc.xpath(
                '//tr[.//*[contains(.//text(), "Place of birth")]]//td//p|//p[.//*[contains(.//text(), "Place of birth")]]'
            )[-1].text_content()
        )
        .replace("Place of birth", "")
        .strip()
    )

    position = collapse_spaces(
        doc.xpath('//div[@class="sn_narys_vardas_title"]')[0].text_content()
    )
    tenure = collapse_spaces(doc.xpath('//div[@class="kadencija"]')[0].text_content())
    party = collapse_spaces(
        doc.xpath('//a[contains(@class, "smn-frakcija")]')[0].text_content()
    )

    print(
        {
            "name": name,
            "date_of_birth": date_of_birth,
            "place_of_birth": place_of_birth,
            "position": position,
            "tenure": tenure,
            "party": party,
        }
    )


def crawl(context: Context) -> Optional[str]:
    doc = context.fetch_html(
        "https://www.lrs.lt/sip/portal.show?p_r=35299&p_k=2", cache_days=1
    )

    for anchor in doc.xpath(
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
    ):
        anchor_url = anchor.get("href")
        crawl_member_bio(context, anchor_url)
