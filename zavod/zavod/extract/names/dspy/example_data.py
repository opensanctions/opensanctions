import json
from hashlib import sha1
from pathlib import Path
from typing import Any

import yaml

import dspy  # type: ignore

FIELDS = ["name", "alias", "weakAlias", "previousName"]
EXAMPLES_PATH = Path(__file__).parent / "single_entity_examples.yml"


SPLITS = ("train", "val", "test")
SPLIT_BOUNDARIES = (0.33, 0.66)
"""Fraction of the hash space below which an example is train, then val; the rest is test."""


def split_for(case: dict[str, Any]) -> str:
    """Deterministically assign an example to a split from a hash of its inputs.

    Assignment depends only on the example itself, so adding or removing other
    examples never moves an existing example between splits, and scores before
    and after adding examples are computed on overlapping test sets.
    """
    key = json.dumps(
        {"entity_schema": case["entity_schema"], "strings": sorted(case["strings"])},
        sort_keys=True,
        ensure_ascii=False,
    )
    digest = sha1(key.encode("utf-8")).digest()
    fraction = int.from_bytes(digest[:8], "big") / 2**64
    if fraction < SPLIT_BOUNDARIES[0]:
        return "train"
    if fraction < SPLIT_BOUNDARIES[1]:
        return "val"
    return "test"


def load_data(
    examples_path: Path,
) -> tuple[list[dspy.Example], list[dspy.Example], list[dspy.Example]]:
    with open(examples_path, encoding="utf-8") as f:
        cases = yaml.load(f, Loader=yaml.SafeLoader)

    splits: dict[str, list[dspy.Example]] = {name: [] for name in SPLITS}
    for case in cases:
        for field in FIELDS:
            if field not in case:
                case[field] = []
        num_names = sum([len(case[field]) for field in FIELDS])
        if num_names == 0:
            continue
        example = dspy.Example(case).with_inputs("strings", "entity_schema")
        splits[split_for(case)].append(example)

    return splits["train"], splits["val"], splits["test"]
