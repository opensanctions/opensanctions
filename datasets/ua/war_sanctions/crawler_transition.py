"""Transitional entry point: run the API crawler and the public crawler in one pass.

During the migration we emit from both `crawler.py` (confidential API, ua-ws-* ids) and
`crawler_zyte.py` (public website, imo-* / ua-ws-* ids) in the same run, so dedupe can link
the old and new entities by their shared IMO / name. Once the clusters are stable, point the
dataset `entry_point` at `crawler_zyte.py` and delete this file together with `crawler.py`.

zavod loads entry-point files by path, not as a package, so a sibling module can't be
imported with a relative (`from . import ...`) or bare (`import crawler`) statement — we
load it by file path via importlib instead.
"""

import importlib.util
from pathlib import Path
from types import ModuleType

from zavod import Context


def _load_sibling(stem: str) -> ModuleType:
    path = Path(__file__).parent / f"{stem}.py"
    spec = importlib.util.spec_from_file_location(f"_ua_ws_{stem}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load sibling crawler: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def crawl(context: Context) -> None:
    _load_sibling("crawler").crawl(context)
    _load_sibling("crawler_zyte").crawl(context)
