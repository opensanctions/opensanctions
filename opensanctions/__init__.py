import os
from memorious.core import manager


def init():
    config_path = os.path.join(os.path.dirname(__file__), "config")
    manager.load_path(config_path)
