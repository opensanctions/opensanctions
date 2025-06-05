import sys
import csv
import yaml
import click
import orjson
import logging
import requests
import jq  # type: ignore
import itertools
from functools import cached_property
from typing import Any, Dict, Generator, List, Literal, Set, Tuple
from pydantic import BaseModel, model_validator
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
    name: str
    path: str
    multi: Literal["row", "column", "join"] = "row"
    join: str = ";"
    max: int = 1000
    unique: bool = False
    sort: bool = False
    repeat: bool = True

    @cached_property
    def program(self) -> jq._Program:
        return jq.compile(self.path)

    @property
    def is_multi_column(self) -> bool:
        return self.multi == "column"

    @property
    def is_multi_row(self) -> bool:
        return self.multi == "row"

    def column_name(self, idx: int = 0) -> str:
        return f"{self.name}_{idx}" if self.is_multi_column else self.name

    def fieldnames(self) -> List[str]:
        if self.is_multi_column:
            return [self.column_name(i) for i in range(self.max)]
        return [self.column_name()]

    def __hash__(self):
        return hash(self.name)


class Mapping(BaseModel):
    url: str
    encoding: str = "utf-8"
    sample_size: int = 10_000
    filters: MappingFilters = MappingFilters()
    dialect: MappingCsvDialect = MappingCsvDialect()
    columns: List[MappingColumn]

    @model_validator(mode="before")
    @classmethod
    def transform_input(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        columns: List[Any] = []
        for name, data in values.get("columns", {}).items():
            data["name"] = name
            columns.append(data)
        values["columns"] = columns
        return values

    @property
    def is_fixed_width(self) -> bool:
        """Check if the mapping is for fixed-width CSV output."""
        for column in self.columns:
            if column.is_multi_column:
                return False
        return True

    def fieldnames(self) -> List[str]:
        """Return all the possible field names for the CSV output."""
        names = []
        for column in self.columns:
            for name in column.fieldnames():
                names.append(name)
        return names


def has_schema(schema: Schema, schemata: List[str]) -> bool:
    if len(schemata) == 0 or schema.name in schemata:
        return True
    return any(schema.is_a(s) for s in schemata)


def stringify(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value).strip()


def transform_object(
    mapping: Mapping, data: Dict[str, Any]
) -> Generator[Dict[str, str], None, None]:
    schema = model.get(data["schema"])
    if schema is None:
        return
    if not has_schema(schema, mapping.filters.schemata):
        return

    properties = data.get("properties", {})
    topics = set(properties.get("topics", []))
    if len(mapping.filters.topics) > 0:
        if len(topics.intersection(mapping.filters.topics)) == 0:
            return

    values: Dict[Tuple[MappingColumn, str], List[str]] = {}
    max_rows = 1
    for column in mapping.columns:
        val: List[str] = column.program.input_value(data).all()
        if len(val) > 0 and isinstance(val[0], list):
            val = list(itertools.chain.from_iterable(val))
        if column.unique:
            val = list(set(val))
        if column.sort:
            val.sort()
        if len(val) > column.max:
            log.warning(
                "Column %s has more than %d values",
                column.name,
                column.max,
            )
            val = val[: column.max]
        if column.is_multi_row:
            max_rows = max(max_rows, len(val))
            values[(column, column.name)] = [stringify(v) for v in val]
        elif column.is_multi_column:
            for idx, v in enumerate(val):
                col_name = column.column_name(idx)
                values[(column, col_name)] = [stringify(v)]
        else:
            val = [column.join.join(stringify(v) for v in val)]
            values[(column, column.name)] = val

    for idx in range(max_rows):
        row: Dict[str, str] = {}
        for (column, name), val in values.items():
            if len(val) > idx:
                value = val[idx]
            elif column.repeat and len(val):
                value = val[-1]
            else:
                value = ""
            row[name] = value
        yield row


def stream_lines(mapping: Mapping) -> Generator[str, None, None]:
    log.info("Fetching data from %r", mapping.url)
    ## load each line as a JSON object
    try:
        response = requests.get(mapping.url, stream=True)
        response.raise_for_status()
        for line in response.iter_lines(chunk_size=10 * 8192):
            if not line:
                continue
            yield line

    except requests.RequestException as e:
        click.echo(f"Error fetching data: {e}", err=True)
        sys.exit(1)


def transform(mapping: Mapping) -> Generator[Dict[str, str], None, None]:
    for idx, line in enumerate(stream_lines(mapping)):
        if idx % 10000 == 0:
            log.info(f"Processed {idx} lines...")
        data = orjson.loads(line)
        for row in transform_object(mapping, data):
            yield row


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
        fieldnames = mapping.fieldnames()
        sample: List[Dict[str, str]] = []
        gen = transform(mapping)
        if not mapping.is_fixed_width:
            sample.extend(itertools.islice(gen, mapping.sample_size))
            seen: Set[str] = set()
            for row in sample:
                seen.update(row.keys())
            fieldnames = [f for f in fieldnames if f in seen]

        writer = csv.DictWriter(
            outfh,
            fieldnames=fieldnames,
            dialect=csv.unix_dialect,
            extrasaction="raise",
            delimiter=dialect.delimiter,
            quotechar=dialect.quotechar,
            escapechar=dialect.escapechar,
            lineterminator=dialect.lineterminator,
            quoting=quoting,
        )
        writer.writeheader()
        for row in sample:
            writer.writerow(row)
        sample.clear()

        for row in gen:
            writer.writerow(row)


if __name__ == "__main__":
    main()
