# import re
# from functools import cache
# from importlib import import_module
# from importlib.util import module_from_spec, spec_from_file_location
# from pathlib import Path
# from typing import Callable

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
