import os
from pathlib import Path
from banal import as_bool
from datetime import datetime

DATA_PATH_ = os.environ.get("ZAVOD_DATA_PATH", "data")
DATA_PATH = Path(DATA_PATH_)

# Per-run timestamp
RUN_TIME = datetime.utcnow().replace(microsecond=0)

HTTP_TIMEOUT = 1200
HTTP_USER_AGENT = "Mozilla/5.0 (zavod)"
HTTP_USER_AGENT = os.environ.get("ZAVOD_HTTP_USER_AGENT", HTTP_USER_AGENT)

LOG_JSON = as_bool(os.environ.get("ZAVOD_LOG_JSON", "false"))
