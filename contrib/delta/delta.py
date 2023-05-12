import os
import sys
import click
import orjson
import logging
import requests
from banal import hash_data
from collections import Counter
from typing import Any, BinaryIO, Dict, Generator
from pathlib import Path
from datetime import datetime, timedelta
from requests.exceptions import RequestException

log = logging.getLogger("delta")
DATE_FORMAT = "%Y%m%d"
URL_PATTERN = "https://data.opensanctions.org/datasets/%s/%s/entities.ftm.json"
DATA_PATH_TXT = os.environ.get("DELTA_DATA_PATH", "data")
DATA_PATH = Path(DATA_PATH_TXT).resolve()

OP_ADD = "ADDED"
OP_REMOVE = "REMOVED"
OP_MOD = "MODIFIED"
OP_MERGED = "MERGED"


def fetch_file(
    url: str,
    name: str,
) -> Path:
    """Fetch a (large) file via HTTP to the data path."""
    out_path = DATA_PATH.joinpath(name)
    if out_path.exists():
        return out_path
    log.info("Fetching: %s", url)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True) as res:
        res.raise_for_status()
        with open(out_path, "wb") as fh:
            for chunk in res.iter_content(chunk_size=8192 * 10):
                fh.write(chunk)
    return out_path


def fetch_release(scope: str, timestamp: str) -> Path:
    url = URL_PATTERN % (timestamp, scope)
    return fetch_file(url, f"{scope}-{timestamp}.json")


def entity_hash(data: Dict[str, Any]) -> str:
    data.pop("first_seen", None)
    data.pop("last_seen", None)
    data.pop("referents", None)
    data.pop("datasets", None)
    data.pop("caption", None)
    data["properties"].pop("modifiedAt", None)
    data["properties"].pop("education", None)
    data["properties"].pop("position", None)
    data["properties"].pop("topics", None)
    return hash_data(data)


def iter_file(path: Path) -> Generator[Dict[str, Any], None, None]:
    with open(path, "rb") as fh:
        while line := fh.readline():
            data = orjson.loads(line)
            yield data


def compute_hashes(path: Path) -> Dict[str, str]:
    log.info("Compute hashes: %s" % path.as_posix())
    hashes: Dict[str, str] = {}
    for ent in iter_file(path):
        hashes[ent["id"]] = entity_hash(ent)
    return hashes


def write_entity(fh: BinaryIO, data: Dict[str, Any], op: str):
    out = {"op": op, "entity": data}
    fh.write(orjson.dumps(out, option=orjson.OPT_APPEND_NEWLINE))


def generate_delta(scope: str, dt: datetime):
    db = dt - timedelta(hours=24)
    cur_ts = dt.strftime(DATE_FORMAT)
    try:
        cur_path = fetch_release(scope, cur_ts)
    except RequestException as re:
        log.warn("Cannot fetch current file [%s]: %s" % (cur_ts, re))
        # Running this in a batch job early in the day becomes a race
        # condition against the ETL.
        sys.exit(0)
    cur_hashes = compute_hashes(cur_path)
    prev_ts = db.strftime(DATE_FORMAT)
    try:
        prev_path = fetch_release(scope, prev_ts)
    except RequestException as re:
        log.error("Cannot fetch previous file [%s]: %s" % (prev_ts, re))
        sys.exit(1)
    prev_hashes = compute_hashes(prev_path)
    entities = set(cur_hashes.keys())
    entities.update(prev_hashes.keys())
    ops: Dict[str, str] = {}
    for entity_id in entities:
        prev_hash = prev_hashes.get(entity_id)
        cur_hash = cur_hashes.get(entity_id)
        if prev_hash == cur_hash:
            continue
        if prev_hash is None:
            ops[entity_id] = OP_ADD
            continue
        if cur_hash is None:
            ops[entity_id] = OP_REMOVE
            continue
        ops[entity_id] = OP_MOD

    # compute merges. this is wonky -
    for ent in iter_file(cur_path):
        entity_id = ent["id"]
        for ref in ent.get("referents", []):
            if ops.get(ref) == OP_REMOVE:
                ops[ref] = OP_MERGED
                ops[entity_id] = OP_MOD
                # log.info("Merged %s -> %s" % (ref, ent['id']))

    counts = Counter(ops.values())
    log.info("Found %d changes: %r", len(ops), counts)

    out_path = DATA_PATH.joinpath(f"delta/{cur_ts}/{scope}.delta.json")
    out_path.parent.mkdir(exist_ok=True, parents=True)
    with open(out_path, "wb") as outfh:
        for ent in iter_file(prev_path):
            if ops.get(ent["id"]) == OP_REMOVE:
                write_entity(outfh, ent, OP_REMOVE)
        for ent in iter_file(cur_path):
            op = ops.get(ent["id"])
            if op is not None:
                write_entity(outfh, ent, op)
    log.info("Wrote: %s" % out_path.as_posix())


@click.command()
@click.argument("dataset", type=str, default="default")
@click.argument("date", type=str, default=datetime.utcnow().strftime(DATE_FORMAT))
def generate_delta_cmd(dataset: str, date: str) -> None:
    logging.basicConfig(level=logging.INFO)
    log.warning("This script uses OpenSanctions CC-BY-NC-licensed data!")
    dt = datetime.strptime(date, DATE_FORMAT)
    generate_delta(dataset, dt)


if __name__ == "__main__":
    generate_delta_cmd()
