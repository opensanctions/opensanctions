import logging
from pathlib import Path

import click
import orjson

log = logging.getLogger("delta")


@click.command()
@click.argument("dataset", type=Path)
def crawl_file(dataset: str) -> None:
    logging.basicConfig(level=logging.INFO)
    with open(dataset, "rb") as fh:
        while line := fh.readline():
            data = orjson.loads(line)
            target = data.get("target", False)
            props = data.get("properties", {})
            topics = props.get("topics", [])
            if target and not len(topics):
                log.info("No topics: %r" % data["datasets"])


if __name__ == "__main__":
    crawl_file()
