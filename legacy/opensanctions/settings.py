# Settings configuration for OpenSanctions
# Below settings can be configured via environment variables, which makes
# them easy to access in a dockerized environment.
from pathlib import Path

# Directory with metadata specifications for each crawler
SETTINGS_PATH = Path(__file__).resolve()
METADATA_PATH = SETTINGS_PATH / ".." / ".." / ".." / "datasets"
METADATA_PATH = METADATA_PATH.resolve()
