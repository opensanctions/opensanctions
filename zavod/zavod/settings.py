from os import environ as env
from pathlib import Path

from banal import as_bool
from nomenklatura.versions import Version
from rigour.env import env_str

# Logging configuration
LOG_JSON = as_bool(env_str("ZAVOD_LOG_JSON", "false"))

# Debug mode
DEBUG = as_bool(env_str("ZAVOD_DEBUG", "false"))

# Default paths
DATA_PATH_ = env_str("ZAVOD_DATA_PATH", "data")
DATA_PATH = Path(env_str("OPENSANCTIONS_DATA_PATH", DATA_PATH_)).resolve()
DATA_PATH.mkdir(parents=True, exist_ok=True)

# Per-run timestamp
RUN_VERSION = Version.from_env("ZAVOD_VERSION")
RUN_TIME = RUN_VERSION.dt
RUN_TIME_ISO = RUN_VERSION.dt.isoformat(sep="T", timespec="seconds")
RUN_DATE = RUN_VERSION.dt.date().isoformat()

ENRICH_TOPICS = {
    "role.pep",
    "role.rca",
    "sanction",
    "sanction.linked",
    "debarment",
    "asset.frozen",
    "poi",
    "gov.soe",
}

# Release version
RELEASE = env_str("ZAVOD_RELEASE", RUN_TIME.strftime("%Y%m%d"))

# Public URL version
ARCHIVE_SITE = env_str("ZAVOD_ARCHIVE_SITE", "https://data.opensanctions.org")
WEB_SITE = env_str("ZAVOD_WEB_SITE", "https://www.opensanctions.org")

# Bucket to back-fill missing data artifacts from
ARCHIVE_BACKEND = env.get("ZAVOD_ARCHIVE_BACKEND", "FileSystemBackend")
ARCHIVE_BUCKET = env.get("ZAVOD_ARCHIVE_BUCKET", None)
ARCHIVE_BUCKET = env.get("OPENSANCTIONS_BACKFILL_BUCKET", ARCHIVE_BUCKET)
ARCHIVE_PATH = Path(env.get("ZAVOD_ARCHIVE_PATH", DATA_PATH.joinpath("archive")))
ARCHIVE_BACKFILL_STATEMENTS = as_bool(env_str("ARCHIVE_BACKFILL_STATEMENTS", "false"))
BACKFILL_RELEASE = env_str("ZAVOD_BACKFILL_RELEASE", "latest")

# HTTP settings
HTTP_TIMEOUT = 1200
HTTP_RETRY_TOTAL = int(env.get("ZAVOD_HTTP_RETRY_TOTAL", 3))
HTTP_RETRY_BACKOFF_FACTOR = float(env.get("ZAVOD_HTTP_RETRY_BACKOFF_FACTOR", 1.0))
# urllib.util.Retry.DEFAULT_BACKOFF_MAX is 120
HTTP_RETRY_BACKOFF_MAX = int(env.get("ZAVOD_HTTP_RETRY_BACKOFF_MAX", 120))

# Database-backed cache settings
DATABASE_URI = f"sqlite:///{DATA_PATH.joinpath('zavod.sqlite3').as_posix()}"
DATABASE_URI = env.get("ZAVOD_DATABASE_URI", DATABASE_URI)
DATABASE_URI = env_str("OPENSANCTIONS_DATABASE_URI", DATABASE_URI)

# pywikibot settings for editing Wikidata
WD_CONSUMER_TOKEN = env.get("ZAVOD_WD_CONSUMER_TOKEN")
WD_CONSUMER_SECRET = env.get("ZAVOD_WD_CONSUMER_SECRET")
WD_ACCESS_TOKEN = env.get("ZAVOD_WD_ACCESS_TOKEN")
WD_ACCESS_SECRET = env.get("ZAVOD_WD_ACCESS_SECRET")
WD_USER = env.get("ZAVOD_WD_USER")

ZYTE_API_KEY = env.get("OPENSANCTIONS_ZYTE_API_KEY", None)
OPENAI_API_KEY = env.get("OPENSANCTIONS_OPENAI_API_KEY", None)
AZURE_OPENAI_ENDPOINT = env.get("OPENSANCTIONS_AZURE_OPENAI_ENDPOINT", None)

# Test code in prod code is generally a Bad Idea.
# This is here to allow for fallbacks to skip some external service usage
# which allows us to run more crawlers in CI without introducing mocking for those runs.
# Test code impacted by this should mock settings.CI to False to verify normal operation.
CI = as_bool(env_str("CI", "false"))
