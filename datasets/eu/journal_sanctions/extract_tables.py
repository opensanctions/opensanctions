import csv
from urllib.parse import parse_qs, urlparse

import click
import requests
from lxml import html
from normality import collapse_spaces, slugify

from zavod import helpers as h


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
                    row.update({k: v.text_content().strip() for k, v in row.items()})
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
