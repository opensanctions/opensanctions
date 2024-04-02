import re
from typing import List
from lxml.etree import _Element

from zavod import Context, helpers as h
from normality import collapse_spaces

REGEX_AND = re.compile(r"(\band\b|&|\+)", re.I)
REGEX_JUST_A_NAME = re.compile(r"^[a-z]+, [a-z]+$", re.I)
REGEX_CLEAN_SUFFIX = re.compile(
    r", \b(LLC|L\.L\.C|Inc|Jr|INC|L\.P|LP|Sr|III|II|IV|S\.A|LTD|USA INC|\(?A/K/A|\(?N\.K\.A|\(?N/K/A|\(?F\.K\.A|formerly known as|INCORPORATED)\b",
    re.I,
)

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Sec-Fetch-Site": "none",
    # 'Cookie': 'ak_bmsc=A3C2A6E89561F03D019B7734CFF74908~000000000000000000000000000000~YAAQhaQSApS3YzeOAQAAQZRchhcbv/xfRTuurBlBJl6VzAfZ/ifQYmrSUNge7/vRJzWj05BGj22SJ/LzW2hkTOz4/JTneZSfo3pDs0yRmzPN06Tc6QxB/NJQJsteHGD8MSznKOUx1vqME1i8bGDOl35p0XWgXLTAx7hk5hASEHylVgn2cnZZ6IJqDpaQSYfjRQANXPeObZ409eo5TEfF49QH6pvvzM2GjCQJfl88KWrrXzOJmBhPuBJ3GlFz6rRVhwN+o9Z6u6MwVc2n5XSW28aZUIQDxxgC/gyhxI+pfypXliyX2+k/Wc7bup/FSoP39MgWqJ46woBjktMm7HtpZ0/Q0HcsLeFK+JnThAhiH9jMK/L94YXnSzRsC/g3tu5FBVhkWWiWhw==',
    # 'Accept-Encoding': 'gzip, deflate, br',
    "Sec-Fetch-Mode": "navigate",
    "Host": "www.sec.gov",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Connection": "keep-alive",
}


def clean_names(context: Context, text: str) -> List[str]:
    text = collapse_spaces(text)
    if not text:
        return []

    text = REGEX_CLEAN_SUFFIX.sub(r" \1", text)
    # If the string ends in a comma, the last comma is unnecessary (e.g. Goldman Sachs & Co. LLC,)
    if text.endswith(","):
        text = text[:-1]

    if not REGEX_AND.search(text) and not REGEX_JUST_A_NAME.match(text):
        names = [n.strip() for n in text.split(",")]
        return names
    else:
        if "," in text:
            res = context.lookup("comma_names", text)
            if res:
                return res.names
            else:
                context.log.warning(
                    "Not sure how to split on comma.", text=text.lower()
                )
                return [text]
        else:
            return [text]


def crawl_item(item: _Element, context: Context):
    names = clean_names(context, item.text_content())

    source_url = item.get("href")

    for name in names:
        entity = context.make("LegalEntity")
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("topics", "crime.fin")
        entity.add("country", "us")

        sanction = h.make_sanction(context, entity)

        sanction.add("sourceUrl", source_url)

        context.emit(entity, target=True)
        context.emit(sanction)


def crawl(context: Context):
    response = context.fetch_html(context.data_url, headers=HEADERS)

    response.make_links_absolute(context.data_url)

    for item in response.findall(".//div[@class='article-body']/ul[2]/li/a"):
        crawl_item(item, context)
