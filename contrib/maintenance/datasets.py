"""Resolve dataset names to their files in this repository checkout."""

import os
from pathlib import Path
from typing import Any

import yaml

# The repo root: GITHUB_WORKSPACE in CI, otherwise the working directory —
# `python -m contrib.maintenance...` must be invoked from the repo root anyway,
# and the relative default keeps emitted paths short (`datasets/...`).
repo_path = Path(os.environ.get("GITHUB_WORKSPACE", "."))
datasets_path = repo_path / "datasets"


def get_path_from_name(name: str) -> str:
    for path in datasets_path.glob("**/*.y*ml"):
        if path.stem == name:
            return path.as_posix()
    raise RuntimeError(f"Dataset {name!r} not found in: {datasets_path}")


def get_code_path(yaml_path: str, entry_point: str | None) -> str | None:
    """Resolve a dataset's entry_point to the crawler source file, if it has one.

    Use this to give an agent the actual code to read and fix, not just the
    metadata YAML. Mirrors zavod's loader (zavod/runtime/loader.py): an
    entry_point naming an installed module — e.g. `zavod.runner.enrich:enrich`,
    used by enrichment datasets — has no dataset-local code to edit, so return
    None. Otherwise the entry_point names a file relative to the dataset
    directory (`crawler.py`, `crawler`, `ofac_advanced.py:crawl`); return its path.
    """
    if entry_point is None:
        return None
    module_name = entry_point.split(":", 1)[0]
    base = Path(yaml_path).parent
    for candidate in (module_name, f"{module_name}.py"):
        file_path = base / candidate
        if file_path.is_file():
            return file_path.as_posix()
    return None


def read_dataset_meta(yaml_path: str) -> dict[str, Any]:
    """Load local operational metadata that is absent from the public catalog."""
    with open(yaml_path, "r") as fh:
        data = yaml.safe_load(fh)
        assert isinstance(data, dict), f"Unexpected YAML structure: {yaml_path}"
        return data
