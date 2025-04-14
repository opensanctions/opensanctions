import csv
from urllib.parse import parse_qs, urlparse

import click
import requests
from lxml import html
from normality import collapse_spaces

from zavod import helpers as h


@click.command()
@click.argument("URL", type=str)
def extract_tables(url: str) -> None:
    """
    Args:
        url: https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=...
    """
    query_params = parse_qs(urlparse(url).query)
    doc_reference = query_params.get("uri")[0].replace(":", "_")

    r = requests.get(url)
    r.raise_for_status()

    doc = html.fromstring(r.text)
    contentContainer = doc.xpath("//div[@id='textTabContent']")
    for i, table in enumerate(contentContainer[0].xpath(".//table")):
        filename = f"{doc_reference}_table_{i}.csv"

        rows = list(h.parse_html_table(table, header_tag="td"))
        if len(rows) == 0:
            # Table for layout, probably
            header = [c.text_content() for c in table.xpath(".//td")]
            sample = collapse_spaces(" | ".join(header)[:150])
            print("Skipping empty table", i, sample, "...")
            continue

        with open(filename, "w") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows:
                row = h.cells_to_str(row)
                writer.writerow(row)
        print(f"Extracted table {i} to {filename}")


if __name__ == "__main__":
    extract_tables()
