import os
import csv
import sys
import click
import orjson
import logging
import requests
import copy
from banal import hash_data
from collections import Counter
from typing import Any, BinaryIO, Dict, Generator
from pathlib import Path
from datetime import datetime, timedelta
from requests.exceptions import RequestException

log = logging.getLogger("delta")
DATE_FORMAT = "%Y%m%d"
DEFAULT_CUR = datetime.utcnow()
DEFAULT_PREV = DEFAULT_CUR - timedelta(days=2)
URL_PATTERN = "https://data.opensanctions.org/datasets/%s/%s/entities.ftm.json"
DATA_PATH_TXT = os.environ.get("DELTA_DATA_PATH", "data")
DATA_PATH = Path(DATA_PATH_TXT).resolve()

OP_ADD = "ADDED"
OP_REMOVE = "REMOVED"
OP_MOD = "MODIFIED"
OP_MERGED = "MERGED"
OPS = (OP_ADD, OP_REMOVE, OP_MERGED, OP_MOD)


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


def is_target(data: Dict[str, Any]) -> bool:
    return data.get("target", False)


def entity_hash(ent: Dict[str, Any]) -> str:
    data = copy.deepcopy(ent)
    data.pop("id", None)
    data.pop("schema", None)
    data.pop("first_seen", None)
    data.pop("last_seen", None)
    data.pop("last_change", None)
    data.pop("referents", None)
    data.pop("datasets", None)
    data.pop("caption", None)
    data.pop("target", None)
    data["properties"].pop("modifiedAt", None)
    data["properties"].pop("education", None)
    data["properties"].pop("position", None)
    data["properties"].pop("topics", None)
    # print(data)
    return hash_data(data)


def iter_file(path: Path) -> Generator[Dict[str, Any], None, None]:
    with open(path, "rb") as fh:
        while line := fh.readline():
            data = orjson.loads(line)
            if is_target(data):
                yield data


def compute_hashes(path: Path, mapping: Dict[str, str]) -> Dict[str, str]:
    log.info("Compute hashes: %s" % path.as_posix())
    hashes: Dict[str, str] = {}
    for ent in iter_file(path):
        ent_id = mapping.get(ent["id"], ent["id"])
        hashes[ent_id] = entity_hash(ent)
    return hashes


def write_entity(fh: BinaryIO, data: Dict[str, Any], op: str):
    out = {"op": op, "entity": data}
    fh.write(orjson.dumps(out, option=orjson.OPT_APPEND_NEWLINE))


def generate_delta(scope: str, cur: datetime, prev: datetime):
    # db = dt - timedelta(hours=24)
    cur_ts = cur.strftime(DATE_FORMAT)
    try:
        cur_path = fetch_release(scope, cur_ts)
    except RequestException as re:
        log.warn("Cannot fetch current file [%s]: %s" % (cur_ts, re))
        # Running this in a batch job early in the day becomes a race
        # condition against the ETL.
        sys.exit(0)
    prev_ts = prev.strftime(DATE_FORMAT)
    try:
        prev_path = fetch_release(scope, prev_ts)
    except RequestException as re:
        log.error("Cannot fetch previous file [%s]: %s" % (prev_ts, re))
        sys.exit(1)

    mapping: Dict[str, str] = {}
    for path in (prev_path, cur_path):
        for ent in iter_file(path):
            for ref in ent.get("referents", []):
                mapping[ref] = ent["id"]

    cur_hashes = compute_hashes(cur_path, mapping)
    prev_hashes = compute_hashes(prev_path, mapping)
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
    for ent in iter_file(prev_path):
        entity_id = ent["id"]
        canonical_id = mapping.get(entity_id, entity_id)
        if canonical_id == entity_id:
            continue
        if ops.get(canonical_id) in (OP_ADD, OP_MOD):
            ops[canonical_id] = OP_MERGED

    counts = Counter(ops.values())
    log.info("Found %d changes: %r", len(ops), counts)

    prev_entities = {}
    for ent in iter_file(prev_path):
        canonical_id = mapping.get(ent["id"], ent["id"])
        if canonical_id in ops:
            prev_entities[canonical_id] = ent

    cur_entities = {}
    for ent in iter_file(cur_path):
        canonical_id = mapping.get(ent["id"], ent["id"])
        if canonical_id in ops:
            cur_entities[canonical_id] = ent

    # for entity_id, op in ops.items():
    #     if op != OP_MOD:
    #         continue
    #     prev_ent = prev_entities[entity_id]
    #     cur_ent = cur_entities[entity_id]
    #     prev_props = prev_ent['properties']
    #     cur_props = cur_ent['properties']
    #     props = set(prev_props.keys())
    #     props.update(cur_props.keys())
    #     diffs = {}
    #     for prop in props:
    #         rem_vals = [v for v in prev_props.get(prop, []) if v not in cur_props.get(prop, [])]
    #         add_vals = [v for v in cur_props.get(prop, []) if v not in prev_props.get(prop, [])]
    #         if len(rem_vals) or len(add_vals):
    #             diffs[prop] = {}
    #         if len(rem_vals):
    #             diffs[prop]["removed"] = rem_vals
    #         if len(add_vals):
    #             diffs[prop]["added"] = add_vals
    #     print(entity_id, diffs)

    out_json = DATA_PATH.joinpath(f"delta/{cur_ts}/{scope}.delta.json")
    out_json.parent.mkdir(exist_ok=True, parents=True)
    with open(out_json, "wb") as outjson:
        for op in OPS:
            for entity_id, op_ in ops.items():
                if op_ != op:
                    continue
                ent = cur_entities.get(entity_id) or prev_entities[entity_id]
                # if op == OP_REMOVE:
                #     log.info(
                #         "Removed [%s:%s]: %s",
                #         ent["id"],
                #         ent["schema"],
                #         ent["caption"],
                #     )
                write_entity(outjson, ent, op)
    log.info("Wrote JSON: %s" % out_json.as_posix())  

    out_csv = DATA_PATH.joinpath(f"delta/{cur_ts}/{scope}.delta.csv")
    out_csv.parent.mkdir(exist_ok=True, parents=True)
    with open(out_csv, "w") as outcsv:
        fields = [
            "op",
            "schema",
            "caption",
            "id",
            "url",
            "topics",
            "datasets",
            "last_change",
        ]
        writer = csv.DictWriter(outcsv, fieldnames=fields)
        writer.writeheader()
        for op in OPS:
            for entity_id, op_ in ops.items():
                if op_ != op:
                    continue
                ent = cur_entities.get(entity_id) or prev_entities[entity_id]
                props = ent.get("properties", {})
                writer.writerow(
                    {
                        "op": op,
                        "schema": ent["schema"],
                        "caption": ent["caption"],
                        "id": ent["id"],
                        "url": f"https://www.opensanctions.org/entities/{ent['id']}/",
                        "topics": ";".join(props.get("topics", [])),
                        "datasets": ";".join(ent.get("datasets", [])),
                        "last_change": ent.get("last_change", None),
                    }
                )
    log.info("Wrote CSV: %s" % out_csv.as_posix())


@click.command()
@click.argument("dataset", type=str, default="default")
@click.option("-p", "previous", type=str, default=DEFAULT_PREV.strftime(DATE_FORMAT))
@click.option("-c", "current", type=str, default=DEFAULT_CUR.strftime(DATE_FORMAT))
def generate_delta_cmd(dataset: str, previous: str, current: str) -> None:
    logging.basicConfig(level=logging.INFO)
    log.warning("This script uses OpenSanctions CC-BY-NC-licensed data!")
    cur = datetime.strptime(current, DATE_FORMAT)
    prev = datetime.strptime(previous, DATE_FORMAT)
    generate_delta(dataset, cur, prev)


if __name__ == "__main__":
    generate_delta_cmd()
