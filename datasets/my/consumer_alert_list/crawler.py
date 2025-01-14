from typing import Generator, Dict
from lxml import html
import re
import json
from normality import collapse_spaces
from followthemoney.types import registry

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html

REGEX_URLS = r"(https?://[^\s]+)"
TABLE_XPATH = ".//div[@class='article-content']//table"


def parse_table(table_data) -> Generator[Dict[str, str], None, None]:
    """
    Parse the table and returns the information as a list of dict

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """
    header_tr = html.fromstring(table_data["header_tr"])
    headers = [th.text_content() for th in header_tr.findall(".//th")]
    for row in table_data["rows"]:
        cells = []
        for el in row:
            cells.append(collapse_spaces(html.fromstring(el).text_content()))
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
    if len(name) > registry.name.max_length:
        entity.add("description", name)
    entity.add("topics", "poi")

    # There can be multiple websites for each entity
    properties_text = input_dict.pop("Website")
    for website in re.findall(REGEX_URLS, properties_text):
        entity.add("website", website.strip(","))
    entity.add("notes", properties_text)

    sanction = h.make_sanction(context, entity)

    # The date is always in the format %Y/%m/00%d %b %Y. For example: 2022/09/0029 Sep 2022
    h.apply_date(
        sanction, "startDate", input_dict.pop("Date Added to Alert List").split(" ")[0]
    )

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(input_dict)


def crawl(context: Context):
    actions = [
        # Wait for jQuery DataTable to instantiate
        {
            "action": "waitForSelector",
            "selector": {
                "type": "xpath",
                "value": "//select[@name='example_length']",
            },
            "timeout": 15,
        },
        # Serialize the full dataset
        {
            "action": "evaluate",
            "source": """
                var dataContainer = document.createElement("script");
                dataContainer.setAttribute("id", "dataContainer");
                document.body.appendChild(dataContainer).textContent = JSON.stringify({
                    "header_tr": table.header()[0].innerHTML,
                    "rows": table.data().toArray(),
                });
            """,
        },
    ]
    data_xpath = ".//script[@id='dataContainer']"
    doc = fetch_html(
        context,
        context.data_url,
        data_xpath,
        actions=actions,
        cache_days=1,
    )

    table_data = json.loads(doc.find(data_xpath).text)

    for item in parse_table(table_data):
        crawl_item(item, context)
