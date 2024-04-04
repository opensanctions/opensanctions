"""
Download JSON from the data.hetq.am API / website
"""

import argparse
import json
import logging
import requests

import lxml.html
from pathlib import Path
from urllib.parse import urlencode
from typing import Literal

WEBPAGE = r"https://data.hetq.am/%(lang)s"
FRONTPAGE = r"https://data.hetq.am/api/v2/%(lang)s/front/filters"
OFFICIALTYPES = r"https://data.hetq.am/api/v1/officialstypes/all"
RELATIONS = r"https://data.hetq.am/%(lang)s/iframes/api/v1/relations2/%(person)d"
LANGUAGE = Literal["en", "hy"]
LOGGER = logging.getLogger("hetq-download")


def build_front_url(lang: LANGUAGE, year: int, offset: int):
    url = FRONTPAGE % {"lang": lang}
    return "?".join(
        (
            url,
            urlencode(
                {
                    "filters[year]": year,
                    "filters[category]": "connections",
                    "order": "DESC",
                    "offset": offset,
                }
            ),
        )
    )


def build_relations_url(lang: LANGUAGE, person_id: int):
    return RELATIONS % {"lang": lang, "person": person_id}


def download_front_page(lang: LANGUAGE, outdir: Path):
    """Download all data from the front page for each year available."""
    # Get the years from the (locally saved because User-Agent) HTML
    with open(f"{lang}.html") as page:
        root = lxml.html.parse(page).getroot()
    yeardiv = root.find_class("filter-year")
    for label in yeardiv[0].findall(".//label"):
        year = int(label.text_content())
        LOGGER.info("Downloading %s data for %d...", lang, year)
        offset = 0
        frontpath = outdir / "front" / f"{year}-{lang}.json"
        frontpath.parent.mkdir(parents=True, exist_ok=True)
        entries = []
        while True:
            r = requests.get(build_front_url(lang=lang, year=year, offset=offset))
            r.raise_for_status()
            these_entries = r.json()
            LOGGER.info("Got %d entries at offset %d...", len(these_entries), offset)
            if len(these_entries) == 0:
                break
            entries.extend(these_entries)
            offset += len(these_entries)
        with open(frontpath, "wt") as outfh:
            json.dump(entries, outfh, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("outdir", help="Path to output directory", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    args.outdir.mkdir(exist_ok=True, parents=True)
    for lang in "en", "hy":
        download_front_page(lang, args.outdir)


if __name__ == "__main__":
    main()
