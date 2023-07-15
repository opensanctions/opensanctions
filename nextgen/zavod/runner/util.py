# import re
# from functools import cache
from importlib import import_module

# from importlib.util import module_from_spec, spec_from_file_location
# from pathlib import Path
# from typing import Callable

from zavod.meta import Dataset

# MODULE_RE = re.compile(r"^[\w\.]+:[\w]+")


# @cache
# def is_module(path: str) -> bool:
#     return bool(MODULE_RE.match(path))


# @cache
# def get_func(path: str) -> Callable:
#     module, func = path.rsplit(":", 1)
#     if is_module(path):
#         module = import_module(module)
#     else:
#         path_ = Path(module)
#         spec = spec_from_file_location(path_.stem, module)
#         module = module_from_spec(spec)
#         spec.loader.exec_module(module)
#     return getattr(module, func)


# TODO: move this to a separate module
def load_method(dataset: Dataset):
    """Load the actual crawler code behind the dataset."""
    method = "crawl"
    module = dataset.entry_point
    if module is None:
        raise RuntimeError("The dataset has no entry point!")
    if ":" in module:
        module, method = module.rsplit(":", 1)
    module_ = import_module(module)
    return getattr(module_, method)
