import logging
import warnings
import urllib3

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("charset_normalizer").setLevel(logging.WARNING)
logging.getLogger("httpstream").setLevel(logging.WARNING)
logging.getLogger("prefixdate").setLevel(logging.ERROR)

warnings.simplefilter("ignore", category=ResourceWarning)
urllib3.disable_warnings()

# import yaml
# yaml.SafeLoader.yaml_implicit_resolvers = {
#     k: [r for r in v if r[0] != "tag:yaml.org,2002:timestamp"]
#     for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()
# }
