import csv
import os
from typing import List, Optional
from urllib.parse import urljoin

import requests
from banal import first
from lxml import html
from lxml.etree import _Element
from normality import slugify
from rigour.mime.types import PDF

from zavod import Context
from zavod import helpers as h

ID_SPLITS = ["(a) ", "(b) ", "(c) ", "(d) ", "(e) ", "(f) ", "(g) ", "(h) ", "(i) ", "(j) ", "(k) ", "(l) ", "(m) ", "(n) ", "(o) ", "(p) ", "(q) ", "(r) ", "(s) ", "(t) ", "(u) ", "(v) ", "(w) ", "(x) ", "(y) ", "(z) ", "a) ", "b) ", "c) ", "d) ", "e) ", "f) ", "g) ", "h) ", "i) ", "j) ", "k) ", "l) ", "m) ", "n) ", "o) ", "p) ", "q) ", "r) ", "s) ", "t) ", "u) ", "v) ", "w) ", "x) ", "y) ", "z) "]

def cell_values(el: _Element) -> List[str]:
    items = [i.text_content().strip() for i in el.findall(".//li")]
    if len(items):
        return items
    text = el.text_content().strip()
    if text == "-":
        return []
    return [text]


def crawl_table(context: Context, csvFilePath: _Element) -> None:
    with open(csvFilePath, 'r', encoding='utf-8') as csvfile:
        rows = csv.DictReader(csvfile)

        for row in rows:
            if "Tarikh Lahir" in row:
                schema = "Person"
                key = "person"
                referenceName = "Rujukan"
            else:
                schema = "Organization"
                key = "group"
                referenceName = "No. Ruj."
            entity = context.make(schema)
            reference = first(row.pop(referenceName))
            entity.id = context.make_slug(key, reference)
    
            entity.add("name", row.pop("Nama").split("@"))
            entity.add("topics", "sanction")
            aliases = [] if row.get('Alias') == '-' else [row.get('Alias')]
            row.pop('Alias', []);
            for alias in h.multi_split(aliases, ID_SPLITS):
                entity.add("alias", alias)
            otherName = [] if row.get('Nama Lain') == '-' else [row.get('Nama Lain')]
            row.pop('Nama Lain', []);
            for otherAlias in h.multi_split(otherName, ID_SPLITS):
                entity.add("alias", otherAlias)
            for address in h.multi_split(row.pop("Alamat"), ID_SPLITS):
                entity.add("address", address)

            if entity.schema.is_a("Person"):
                entity.add("title", row.pop("Gelaran", None))
                entity.add("birthDate", row.pop("Tarikh Lahir", None).split(" "))
                entity.add("birthPlace", row.pop("Tempat Lahir", None))
                entity.add("nationality", row.pop("Warganegara", None))
                entity.add("passportNumber", row.pop("Nombor Pasport", None))
                for id in h.multi_split(row.pop("Nombor. Kad Pengenalan", None), ID_SPLITS):
                    entity.add("idNumber", id)

            sanction = h.make_sanction(context, entity)
            sanction.add("listingDate", row.pop("Tarikh Disenaraikan", None).split(" "))
            sanction.add("authorityId", reference)
            sanction.add("program", row.pop("Jawatan", None))
            
            context.emit(entity)
            context.emit(sanction)
            
            # Audit remaining data
            context.audit_data(row, ignore=["No."])


def crawl_html_url(context: Context) -> str:
    # Fetch the initial page
    response = requests.get(context.data_url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    # Parse the HTML
    doc = html.fromstring(response.text)
    
    # Find the validator text
    validator_xpath = ".//*[contains(text(), 'LIST OF SANCTIONS UNDER THE MINISTRY OF HOME AFFAIRS (MOHA)')]"
    if not doc.xpath(validator_xpath):
        raise ValueError("Validator text not found")
    
    for a in doc.findall('.//div[@class="uk-container"]//a'):
        if "sanctions list" not in a.text_content().lower():
            continue
        if ".pdf" in a.get("href", ""):
            print(f"Link text: '{context.data_url}', href: '{a.get("href")}'")
            return urljoin(context.data_url, a.get("href"))
    
    raise ValueError("No HTML found")


def crawl(context: Context):
    html_url = crawl_html_url(context)
    path = context.fetch_resource("source.pdf", html_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    if __package__ is None or __package__ == "":
        import sys
        from pathlib import Path

        # crawler_test.py is at opensanctions/datasets/my/moha_sanctions/
        # parents[3] -> opensanctions/
        ROOT = Path(__file__).resolve().parents[3]
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        from datasets.my.moha_sanctions.pdf_to_csv import (extract_pdf_tables,
                                                           save_tables_to_csv)
    else:
        from .pdf_to_csv import extract_pdf_tables, save_tables_to_csv

    tables = extract_pdf_tables(path)
    save_tables_to_csv(tables)

    crawl_table(context, 'data/datasets/my_moha_sanctions/INDIVIDU.csv')
    crawl_table(context, 'data/datasets/my_moha_sanctions/KUMPULAN.csv')