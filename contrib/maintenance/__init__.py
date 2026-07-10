"""Dataset maintenance tooling: runtime diagnostics and the issues agent.

Modules in this package are invoked from the repository root, e.g.:

    python -m contrib.maintenance.diagnose <dataset_name>
    python -m contrib.maintenance.issues_agent

They deliberately avoid importing zavod so they start fast and run with just
`requests`, `pyyaml` and `jinja2` installed (what the issues-agent CI job has).
"""

import requests

session = requests.Session()
session.headers.update({"User-Agent": "os-maintenance/1.0"})
