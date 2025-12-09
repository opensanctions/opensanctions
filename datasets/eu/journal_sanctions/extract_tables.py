# Tries to extract tables from EU journal sanctions HTML pages.
# Tries to map common key/value pairs in a cell to distinct columns.
#
# Usage:
#   python extract_tables.py --url "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32022R2092"
#
# They seem to be introducing Cloudfront WAF - if you encounter an empty response,
# save as "Complete" from your browser,then use --path to the .htm file


import csv
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import click
import requests
from lxml import html
from normality import collapse_spaces, slugify, squash_spaces
from followthemoney.cli.util import InPath

from zavod import helpers as h


def multiline_squash_spaces(text: str) -> str:
    """Squash spaces on each line of a multiline string."""
    return "\n".join(squash_spaces(line) for line in text.split("\n")).strip()


def extract_identifying(identifying_information: str) -> dict[str, str]:
    other = []
    result = {}
    for line in identifying_information.split("\n"):
        if ":" in line:
            key, value = line.split(":", 1)
            key = slugify(key, sep="_")
            result[key] = value.strip()
        else:
            if line:
                other.append(line.strip())
    if len(other) > 0:
        result["other"] = "\n".join(other)
    return result


@click.command()
@click.option("--url", type=str)
@click.option("--path", type=InPath)
def extract_tables(url: Optional[str], path: Optional[Path]) -> None:
    """
    Args:
        url: https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=...
    """

    if url:
        r = requests.get(url)
        r.raise_for_status()
        html_content = r.text
        query_params = parse_qs(urlparse(url).query)
        doc_reference = query_params.get("uri")[0].replace(":", "_")
    else:
        with open(path, "r") as f:
            html_content = f.read()
        doc_reference = path.stem.replace(":", "_").replace(" ", "_")

    doc = html.fromstring(html_content)
    contentContainer = doc.xpath("//div[@id='textTabContent']")
    for i, table in enumerate(
        contentContainer[0].xpath(".//table[contains(@class, 'oj-table')]")
    ):
        filename = f"{doc_reference}_table_{i}.csv"

        try:
            rows = list(
                h.parse_html_table(table, header_tag="td", index_empty_headers=True)
            )
            if len(rows) == 0:
                # Table for layout, probably
                header = [c.text_content() for c in table.xpath(".//td")]
                sample = collapse_spaces(" | ".join(header)[:150])
                print("Skipping empty table", i, sample, "...")
                continue

            fieldnames = [None, "type"]
            with open(filename, "w") as f:
                for row in rows:
                    row.update(
                        {
                            k: multiline_squash_spaces(v.text_content())
                            for k, v in row.items()
                        }
                    )
                    row["type"] = ""
                    row.update(
                        extract_identifying(row.pop("identifying_information", ""))
                    )

                    for key in row.keys():
                        if key not in fieldnames:
                            fieldnames.append(key)

                if "reasons" in fieldnames:
                    fieldnames.remove("reasons")
                    fieldnames.append("reasons")
                if "date_of_listing" in fieldnames:
                    fieldnames.remove("date_of_listing")
                    fieldnames.append("date_of_listing")

                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

            print(f"Extracted table {i} to {filename}")
        except Exception as e:
            print(f"Error extracting table {i}: {type(e)} {str(e)[:150]}...")
            continue

    print(
        "\nRemember to make sure you've got all tables, and also changes listed not in tables."
    )


if __name__ == "__main__":
    extract_tables()
