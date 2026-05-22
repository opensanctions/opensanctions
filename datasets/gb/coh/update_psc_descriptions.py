"""Refresh psc_descriptions.json from the canonical Companies House enumeration.

Companies House publishes the PSC nature-of-control taxonomy in
`companieshouse/api-enumerations`. The crawler reads it from a vendored JSON
snapshot (rather than fetching live) so each run is reproducible and offline.
Re-run this script when a new slug needs to be supported. JSON, not YAML, so
that the CI's dataset discovery (`contrib/ci_datasets.py`) does not try to
load this data file as a `DatasetModel`.
"""

import json
from pathlib import Path
from urllib.request import urlopen

import yaml

URL = "https://raw.githubusercontent.com/companieshouse/api-enumerations/master/psc_descriptions.yml"
OUT = Path(__file__).parent / "psc_descriptions.json"

if __name__ == "__main__":
    data = yaml.safe_load(urlopen(URL))
    OUT.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")
    print(f"wrote {OUT} ({len(data['description'])} slugs)")
