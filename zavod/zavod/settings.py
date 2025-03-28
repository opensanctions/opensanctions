from os import environ as env

from pathlib import Path
from banal import as_bool
from rigour.env import env_str, env_int
from nomenklatura.versions import Version


# Logging configuration
LOG_JSON = as_bool(env_str("ZAVOD_LOG_JSON", "false"))

# Debug mode
DEBUG = as_bool(env_str("ZAVOD_DEBUG", "false"))

# Default paths
DATA_PATH_ = env_str("ZAVOD_DATA_PATH", "data")
DATA_PATH = Path(env_str("OPENSANCTIONS_DATA_PATH", DATA_PATH_)).resolve()
DATA_PATH.mkdir(parents=True, exist_ok=True)

# Per-run timestamp
TIME_ZONE = env_str("TZ", "UTC")
RUN_VERSION = Version.from_env("ZAVOD_VERSION")
RUN_TIME = RUN_VERSION.dt
RUN_TIME_ISO = RUN_VERSION.dt.isoformat(sep="T", timespec="seconds")
RUN_DATE = RUN_VERSION.dt.date().isoformat()

# Risk categories
TARGET_TOPICS = {
    "corp.disqual",
    "crime.boss",
    "crime.fin",
    "crime.fraud",
    "crime.terror",
    "crime.theft",
    "crime.traffick",
    "crime.war",
    "crime",
    "debarment",
    "export.control",
    "export.risk",
    "poi",
    "reg.action",
    "reg.warn",
    "role.oligarch",
    "role.pep",
    "role.rca",
    "sanction.counter",
    "sanction.linked",
    "sanction",
    "wanted",
}
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


# Store configuration
STORE_RETAIN_DAYS = int(env_str("ZAVOD_STORE_RETAIN_DAYS", "3"))

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
BACKFILL_RELEASE = env_str("ZAVOD_BACKFILL_RELEASE", "latest")

# HTTP settings
HTTP_TIMEOUT = 1200
HTTP_USER_AGENT = "Mozilla/5.0 (zavod)"
HTTP_USER_AGENT = env_str("ZAVOD_HTTP_USER_AGENT", HTTP_USER_AGENT)
HTTP_RETRY_TOTAL = int(env.get("ZAVOD_HTTP_RETRY_TOTAL", 3))
HTTP_RETRY_BACKOFF_FACTOR = float(env.get("ZAVOD_HTTP_RETRY_BACKOFF_FACTOR", 1.0))
# urllib.util.Retry.DEFAULT_BACKOFF_MAX is 120
HTTP_RETRY_BACKOFF_MAX = int(env.get("ZAVOD_HTTP_RETRY_BACKOFF_MAX", 120))

# Database-backed cache settings
DATABASE_URI = f"sqlite:///{DATA_PATH.joinpath('zavod.sqlite3').as_posix()}"
DATABASE_URI = env.get("ZAVOD_DATABASE_URI", DATABASE_URI)
DATABASE_URI = env_str("OPENSANCTIONS_DATABASE_URI", DATABASE_URI)
DB_STMT_TIMEOUT = env_int("ZAVOD_DB_STMT_TIMEOUT", 10000)

# Load DB batch size
DB_BATCH_SIZE = int(env_str("ZAVOD_DB_BATCH_SIZE", "1000"))

OPENSANCTIONS_API_URL = env.get(
    "ZAVOD_OPENSANCTIONS_API_URL", "https://api.opensanctions.org"
)
OPENSANCTIONS_API_KEY = env.get("ZAVOD_OPENSANCTIONS_API_KEY", None)

SYNC_POSITIONS = as_bool(env_str("ZAVOD_SYNC_POSITIONS", "true"))

# pywikibot settings for editing Wikidata
WD_CONSUMER_TOKEN = env.get("ZAVOD_WD_CONSUMER_TOKEN")
WD_CONSUMER_SECRET = env.get("ZAVOD_WD_CONSUMER_SECRET")
WD_ACCESS_TOKEN = env.get("ZAVOD_WD_ACCESS_TOKEN")
WD_ACCESS_SECRET = env.get("ZAVOD_WD_ACCESS_SECRET")
WD_USER = env.get("ZAVOD_WD_USER")

ZYTE_API_KEY = env.get("OPENSANCTIONS_ZYTE_API_KEY", None)
OPENAI_API_KEY = env.get("OPENSANCTIONS_OPENAI_API_KEY", None)
AZURE_OPENAI_ENDPOINT = env.get("OPENSANCTIONS_AZURE_OPENAI_ENDPOINT", None)

ENABLE_SENTRY = as_bool(env.get("ZAVOD_ENABLE_SENTRY", False))
SENTRY_DSN = env.get("ZAVOD_SENTRY_DSN", None)
SENTRY_ENVIRONMENT = env.get("ZAVOD_SENTRY_ENVIRONMENT", None)
