from normality import collapse_spaces
from pantomime.types import CSV
from typing import Dict
import csv

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


def crawl_row(context: Context, row: Dict[str, str]):
    if row.pop("Status") != "Position for territory":
        return
    
    position = h.make_position(
        context,
        row.pop("Position"),
        country=row.pop("Eurostat country code"),
    )
    description_alt_lang = collapse_spaces(row.pop("Description alt lang"))
    if description_alt_lang:
        position.add("description", description_alt_lang, lang=row.pop("alt lang"))
    categorisation = categorise(context, position, True)
    if categorisation.is_pep:
        context.emit(position)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)