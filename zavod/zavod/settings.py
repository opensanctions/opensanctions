from os import environ as env
from pathlib import Path
from banal import as_bool
from normality import stringify

from nomenklatura.versions import Version


def env_str(name: str, default: str) -> str:
    """Ensure the env returns a string even on Windows (#100)."""
    value = stringify(env.get(name))
    return default if value is None else value


# Logging configuration
LOG_JSON = as_bool(env_str("ZAVOD_LOG_JSON", "false"))

# Debug mode
DEBUG = as_bool(env_str("ZAVOD_DEBUG", "false"))

# Default paths
DATA_PATH_ = env_str("ZAVOD_DATA_PATH", "data")
DATA_PATH = Path(env_str("OPENSANCTIONS_DATA_PATH", DATA_PATH_)).resolve()

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

# File path for the resolver path used for entity deduplication
RESOLVER_PATH = env.get("ZAVOD_RESOLVER_PATH")
RESOLVER_PATH = env.get("OPENSANCTIONS_RESOLVER_PATH", RESOLVER_PATH)

# HTTP settings
HTTP_TIMEOUT = 1200
HTTP_USER_AGENT = "Mozilla/5.0 (zavod)"
HTTP_USER_AGENT = env_str("ZAVOD_HTTP_USER_AGENT", HTTP_USER_AGENT)

# Database-backed cache settings
DATABASE_URI = env.get("ZAVOD_DATABASE_URI")
DATABASE_URI = env.get("OPENSANCTIONS_DATABASE_URI", DATABASE_URI)

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
