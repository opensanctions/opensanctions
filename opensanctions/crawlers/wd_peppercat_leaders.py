import csv

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.wikidata import get_entity, entity_to_ftm

CURRENT = "https://raw.githubusercontent.com/every-politician-scrapers/%s/main/html/current.csv"


def crawl_country(context: Context, country: str, url: str):
    res = context.http.get(url, stream=True)
    lines = (line.decode("utf-8") for line in res.iter_lines())
    context.log.info("Importing current leaders", country=country)
    for row in csv.DictReader(lines):
        data = get_entity(row.get("personID"))
        if data is not None:
            entity_to_ftm(
                context,
                data,
                position=data.get("position"),
                topics="role.pep",
                country=country,
            )


def crawl(context: Context):
    params = {"_": settings.RUN_DATE}
    res = context.http.get(context.dataset.data.url, params=params, stream=True)
    lines = (line.decode("utf-8") for line in res.iter_lines())
    for row in csv.DictReader(lines):
        url = CURRENT % row.get("repo")
        crawl_country(context, row.get("country"), url)
