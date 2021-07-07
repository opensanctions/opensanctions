import yaml


def load_yaml(file_path):
    """Safely parse a YAML document."""
    with open(file_path, "r") as fh:
        return yaml.load(fh, Loader=yaml.SafeLoader)
