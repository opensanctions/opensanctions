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
from typing import Literal, Set

WEBPAGE = r"https://data.hetq.am/%(lang)s"
FRONTPAGE = r"https://data.hetq.am/api/v2/%(lang)s/front/filters"
OFFICIALTYPES = r"https://data.hetq.am/api/v1/officialstypes/all"
PERSONPAGE = r"https://data.hetq.am/%(lang)s/profile/%(person)d"
RELATIONS = r"https://data.hetq.am/%(lang)s/iframes/api/v1/relations2/%(person)d"
LANGUAGE = Literal["en", "hy"]
LOGGER = logging.getLogger("hetq-download")
CHROME_HEADER = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}


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
    pagepath = outdir / "front" / f"{lang}.html"
    if pagepath.exists():
        LOGGER.info("Skipping existing file %s", pagepath)
    else:
        url = WEBPAGE % {"lang": "" if lang == "hy" else lang}
        r = requests.get(url, headers=CHROME_HEADER)
        r.raise_for_status()
        with open(pagepath, "wt") as page:
            page.write(r.text)
    with open(pagepath, "rt") as page:
        root = lxml.html.parse(page).getroot()
    yeardiv = root.find_class("filter-year")
    for label in yeardiv[0].findall(".//label"):
        year = int(label.text_content())
        LOGGER.info("Downloading %s data for %d...", lang, year)
        offset = 0
        frontpath = outdir / "front" / f"{year}-{lang}.json"
        if frontpath.exists():
            LOGGER.info("Skipping existing file %s", frontpath)
            continue
        frontpath.parent.mkdir(parents=True, exist_ok=True)
        entries = []
        while True:
            # Will definitely use requests here...
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


def download_people_pages(lang: LANGUAGE, outdir: Path):
    """Get the page for each PEP (no JSON available it seems)."""
    frontdir = outdir / "front"
    # Merge all of them to get unique person ids
    person_ids: Set[int] = set()
    for path in frontdir.glob("*.json"):
        with open(path, "rt") as infh:
            person_ids.update(e["personID"] for e in json.load(infh))
    LOGGER.info("Got %d unique person IDs for %s", len(person_ids), lang)
    (outdir / "person").mkdir(parents=True, exist_ok=True)
    for person in person_ids:
        personpath = outdir / "person" / f"{person}-{lang}.html"
        if personpath.exists():
            LOGGER.info("Skipping existing file %s", personpath)
            continue
        url = PERSONPAGE % {"lang": lang, "person": person}
        LOGGER.info("Downloading profile from %s", url)
        r = requests.get(url, headers=CHROME_HEADER)
        if r.status_code == 404:
            r.raise_for_status()
        elif r.status_code != 200:
            LOGGER.warning("Got weird status %d for %s", r.status_code, url)
        with open(personpath, "wt") as page:
            page.write(r.text)
    return person_ids


def download_relations_pages(person_ids: Set[int], lang: LANGUAGE, outdir: Path):
    """Get relation graph for each PEP (in JSON not SVG thankfully)."""
    (outdir / "relations").mkdir(parents=True, exist_ok=True)
    for person in person_ids:
        personpath = outdir / "relations" / f"{person}-{lang}.json"
        if personpath.exists():
            LOGGER.info("Skipping existing file %s", personpath)
            continue
        url = RELATIONS % {"lang": lang, "person": person}
        LOGGER.info("Downloading relations graph from %s", url)
        r = requests.get(url)
        if r.status_code == 404:
            r.raise_for_status()
        elif r.status_code != 200:
            LOGGER.warning("Got weird status %d for %s", r.status_code, url)
        with open(personpath, "wt") as page:
            json.dump(r.json(), page, indent=2, ensure_ascii=False)


def download_officialtypes(outdir: Path):
    """Get the list of official types (just for completeness really)"""
    officialtypes = outdir / "officialtypes.json"
    if officialtypes.exists():
        LOGGER.info("Skipping existing file %s", officialtypes)
        return
    url = OFFICIALTYPES
    LOGGER.info("Downloading official types from %s", url)
    r = requests.get(url)
    if r.status_code == 404:
        r.raise_for_status()
    elif r.status_code != 200:
        LOGGER.warning("Got weird status %d for %s", r.status_code, url)
    with open(officialtypes, "wt") as page:
        json.dump(r.json(), page, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("outdir", help="Path to output directory", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    args.outdir.mkdir(exist_ok=True, parents=True)
    download_officialtypes(args.outdir)
    for lang in "en", "hy":
        download_front_page(lang, args.outdir)
        person_ids = download_people_pages(lang, args.outdir)
        download_relations_pages(person_ids, lang, args.outdir)


if __name__ == "__main__":
    main()
