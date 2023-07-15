# Settings configuration for OpenSanctions
# Below settings can be configured via environment variables, which makes
# them easy to access in a dockerized environment.
from pathlib import Path
from datetime import datetime
from os import environ as env

from nomenklatura import db
from zavod.settings import env_str

# SQL database URI for structured data
DATABASE_URI = env.get("OPENSANCTIONS_DATABASE_URI")
if DATABASE_URI is None:
    raise RuntimeError("Please set $OPENSANCTIONS_DATABASE_URI.")
if not DATABASE_URI.startswith("postgres"):
    raise RuntimeError("Unsupported database engine: %s" % DATABASE_URI)
db.DB_URL = DATABASE_URI
db.POOL_SIZE = int(env_str("OPENSANCTIONS_POOL_SIZE", db.POOL_SIZE))

# Per-run timestamp
RUN_TIME = datetime.utcnow().replace(microsecond=0)
RUN_TIME_ISO = RUN_TIME.isoformat(sep="T", timespec="seconds")
RUN_DATE = RUN_TIME.date().isoformat()

# Directory with metadata specifications for each crawler
METADATA_PATH = Path(__file__).resolve().parent.joinpath("metadata")
METADATA_PATH = Path(env.get("OPENSANCTIONS_METADATA_PATH", METADATA_PATH)).resolve()

# Resolver file path
RESOLVER_PATH = env.get("OPENSANCTIONS_RESOLVER_PATH")
if RESOLVER_PATH is None:
    raise RuntimeError("Please set $OPENSANCTIONS_RESOLVER_PATH.")
