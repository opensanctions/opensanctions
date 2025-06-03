import sys
import csv
import yaml
import click
import orjson
import logging
import requests
import jmespath
from functools import cached_property
from jmespath.parser import ParsedResult
from datetime import datetime
from typing import Any, Dict, Generator, List, Literal, Optional
from pydantic import BaseModel
from followthemoney import model
from followthemoney.schema import Schema

log = logging.getLogger("export_flatten")
DATA_URL = "https://data.opensanctions.org/datasets/latest/%s/targets.nested.json?v=%s"


### Mapping spec


class MappingFilters(BaseModel):
    topics: List[str] = []
    schemata: List[str] = []


class MappingCsvDialect(BaseModel):
    delimiter: str = ","
    quotechar: str = '"'
    escapechar: str = "\\"
    lineterminator: str = "\n"
    quoting: Literal["minimal", "all"] = "minimal"


class MappingColumn(BaseModel):
    path: str
    multi: Literal["row", "column", "join"] = "row"
    join: str = ";"
    max: int = 1000
    unique: bool = False
    sort: bool = False

    @cached_property
    def expr(self) -> ParsedResult:
        return jmespath.compile(self.path)


class Mapping(BaseModel):
    dataset: str
    encoding: str = "utf-8"
    filters: MappingFilters = MappingFilters()
    dialect: MappingCsvDialect = MappingCsvDialect()
    columns: Dict[str, MappingColumn] = {}


def has_schema(schema: Schema, schemata: List[str]) -> bool:
    if len(schemata) == 0 or schema.name in schemata:
        return True
    return any(schema.is_a(s) for s in schemata)


def transform_object(
    mapping: Mapping, data: Dict[str, Any]
) -> Optional[Dict[str, str]]:
    schema = model.get(data["schema"])
    if schema is None:
        return None
    if not has_schema(schema, mapping.filters.schemata):
        return None

    properties = data.get("properties", {})
    topics = set(properties.get("topics", []))
    if len(mapping.filters.topics) > 0:
        if len(topics.intersection(mapping.filters.topics)) == 0:
            return None

    row = {}
    for name, column in mapping.columns.items():
        res = column.expr.search(data)
        if isinstance(res, str):
            row[name] = res
        # print(values, type(values))
    return row


def transform(mapping: Mapping) -> Generator[Dict[str, str], None, None]:
    url = DATA_URL % (mapping.dataset, datetime.now().strftime("%Y%m%d%H%M"))
    log.info("Fetching data from %r", url)
    ## load each line as a JSON object
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        for idx, line in enumerate(response.iter_lines(chunk_size=10 * 8192)):
            if not line:
                continue
            if idx > 0 and idx % 10000 == 0:
                log.info(f"Processed {idx} lines...")
            data = orjson.loads(line)
            row = transform_object(mapping, data)
            if row is not None:
                yield row

    except requests.RequestException as e:
        click.echo(f"Error fetching data: {e}", err=True)
        sys.exit(1)


@click.command()
@click.argument("mapping", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.argument(
    "outfile", type=click.Path(dir_okay=False, writable=True, readable=False)
)
def main(mapping, outfile):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    try:
        with open(mapping, "r") as file:
            mapping_config = yaml.safe_load(file)
            mapping = Mapping.model_validate(mapping_config)
    except Exception as e:
        click.echo(f"Error loading mapping file: {e}", err=True)
        sys.exit(1)

    with open(outfile, "w", encoding=mapping.encoding) as outfh:
        dialect = mapping.dialect
        quoting = csv.QUOTE_MINIMAL if dialect.quoting == "minimal" else csv.QUOTE_ALL
        writer = csv.DictWriter(
            outfh,
            fieldnames=[],
            dialect=csv.unix_dialect,
            extrasaction="raise",
            delimiter=dialect.delimiter,
            quotechar=dialect.quotechar,
            escapechar=dialect.escapechar,
            lineterminator=dialect.lineterminator,
            quoting=quoting,
        )

        for row in transform(mapping):
            pass


if __name__ == "__main__":
    main()
