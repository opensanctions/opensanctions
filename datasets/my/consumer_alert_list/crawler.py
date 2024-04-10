from typing import Generator, Dict
from lxml.etree import _Element
from lxml import html
import requests
import re
from zavod import Context, helpers as h
from normality import collapse_spaces

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
}

COOKIES = {
    "AWSALBAPP-0": "AAAAAAAAAAARErbtm8bjoqFdM0rg+RA7PxUsebtFiIsR+AZH6gNtNEMMnefAlWYpVhWN+3ADuEn5bG4e7Lgz0QlLwuXXLqVJQrnPOlX1wSCzZDE+ZG/YvuRKjg807MnUW9BrvYIH6v0gUw==",
    "AWSALBAPP-1": "_remove_",
    "AWSALBAPP-2": "_remove_",
    "AWSALBAPP-3": "_remove_",
    "AWSALBTG": "EXH0gauKd/AG8ICFqKlhWbLgoT6EBi0JPWjHDErAGlCogeZgX/OUDZtQONJQAGYwGMnJ44fyIiHR0u8bXNS4dYMDl0cza/GzwCxzn6VkO9bbl3f0lg2qFkt9+APsFDcLUgmp28Z7xGTJtG7OdtNvOUyVl7esufJy9iZ2slTJRuon3m2gKV4=",
    "AWSALBTGCORS": "EXH0gauKd/AG8ICFqKlhWbLgoT6EBi0JPWjHDErAGlCogeZgX/OUDZtQONJQAGYwGMnJ44fyIiHR0u8bXNS4dYMDl0cza/GzwCxzn6VkO9bbl3f0lg2qFkt9+APsFDcLUgmp28Z7xGTJtG7OdtNvOUyVl7esufJy9iZ2slTJRuon3m2gKV4=",
    "LFR_SESSION_STATE_20106": "1712753949135",
    "GUEST_LANGUAGE_ID": "en_US",
    "JSESSIONID": "H-cppef7YvTEyA0ul0f1r9yYan_sJSW2WtrKcXa4.liferay1",
    "aws-waf-token": "2a32b599-9466-4869-94ac-24d8e41d3959:EQoAizBaZNQPAAAA:ZQI0Kf/4O/qihcseS7JYurUhIKBoyEek/fKFdUfe31/FSEcAqRsjzsoXItgcasvngDSGx19DhXRLB5pJ/vvTqu+CnCDz0Vx08/r747qJg8HbFUq8lYKqMjtDC/Kb8qgOO+qPjUFk21jv6S4oonutsJ7A+OmPHBT5dfe76ipra8/48ByMFhwiST6MSzHNCLT6lfdYdo663vqnBRtrQMKJGYoCktGsMDcWRN2CcPcCejp8ENF7",
    "COOKIE_SUPPORT": "true",
}

REGEX_URLS = r"(https?://[^\s]+)"


def parse_table(table: _Element) -> Generator[Dict[str, str], None, None]:
    """
    Parse the table and returns the information as a list of dict

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """
    headers = [th.text_content() for th in table.findall(".//*/th")]
    for row in table.findall(".//*/tr")[1:]:
        cells = []
        for el in row.findall(".//td"):
            cells.append(collapse_spaces(el.text_content()))
        assert len(cells) == len(headers)

        # The table has a last row with all empty values
        if all(c == "" for c in cells):
            continue

        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("Name of unauthorised entities/individual")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)

    entity.add("name", name)

    # There can be multiple websites for each entity
    for website in re.findall(REGEX_URLS, input_dict.pop("Website")):
        entity.add("website", website)

    sanction = h.make_sanction(context, entity)

    # The date is always in the format %Y/%m/00%d %b %Y. For example: 2022/09/0029 Sep 2022
    sanction.add(
        "startDate",
        h.parse_date(
            input_dict.pop("Date Added to Alert List").split(" ")[0],
            formats=["%Y/%m/00%d"],
        ),
    )

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(input_dict)


def crawl(context: Context):
    response = requests.get(context.data_url, headers=HEADERS, cookies=COOKIES)

    doc = html.fromstring(response.text)

    for item in parse_table(doc.find(".//table")):
        crawl_item(item, context)
