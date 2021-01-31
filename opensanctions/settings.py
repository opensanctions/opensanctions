# Settings configuration for OpenSanctions
# Below settings can be configured via environment variables, which makes
# them easy to access in a dockerized environment.
import os
from pathlib import Path
from os import environ as env

# Update the data every day (not totally trying to make this flexible yet,
# since a day seems like a pretty good universal rule).
INTERVAL = 84600

# HTTP cache expiry may last multiple runs
CACHE_EXPIRE = INTERVAL * 10

# Data storage / output location (e.g. a Docker volume mount)
DATA_PATH = Path.cwd().joinpath("data")
DATA_PATH = env.get("OPENSANCTIONS_DATA_PATH", DATA_PATH)
DATA_PATH = Path(DATA_PATH).resolve()

# FIXME: put into some init()
DATA_PATH.mkdir(parents=True, exist_ok=True)

# SQL database URI for structured data
DATABASE_URI = f"sqlite:///{DATA_PATH}/opensanctions.sqlite3"
DATABASE_URI = env.get("OPENSANCTIONS_DATABASE_URI", DATABASE_URI)

# Directory with metadata specifications for each crawler
METADATA_PATH = Path(__file__).resolve().parent.joinpath("metadata")
METADATA_PATH = env.get("OPENSANCTIONS_METADATA_PATH", METADATA_PATH)
METADATA_PATH = Path(METADATA_PATH).resolve()

# User agent
USER_AGENT = "OpenSanctions/3.0"
