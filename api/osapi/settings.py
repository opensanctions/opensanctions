from importlib.metadata import metadata
from opensanctions.settings import env_str

meta = metadata("opensanctions")
VERSION = meta["Version"]
AUTHOR = meta["Author"]
HOME_PAGE = meta["Home-page"]
EMAIL = meta["Author-email"]
CONTACT = {"name": AUTHOR, "url": HOME_PAGE, "email": EMAIL}


SCOPE_DATASET = env_str("OSAPI_SCOPE_DATASET", "default")
ENDPOINT_URL = env_str("OSAPI_ENDPOINT_URL", "http://localhost:8000")
BASE_SCHEMA = "Thing"
