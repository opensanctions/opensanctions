from zavod import Context
from lxml import etree

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://apcis.tmou.org",
    "Referer": "https://apcis.tmou.org/isss/public_apcis.php?Mode=DetList",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}

data = {
    "MIME Type": "application/x-www-form-urlencoded",
    "Mode": "DetList",
    "MOU": "TMOU",
    # "Auth": "Src",
    # "Src": "online",
    "Type": "Auth",
    "Month": "04",
    "Year": "2025",
}


def crawl(context: Context):
    doc = context.fetch_html(
        context.data_url,
        method="POST",
        headers=headers,
        data=data,
    )
    html_string = etree.tostring(
        doc,
        pretty_print=True,
        encoding="unicode",
    )

    print(html_string)
