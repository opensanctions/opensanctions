import re
from pathlib import Path
from types import ModuleType
from typing import Callable, Any, Optional, cast
from importlib import import_module, invalidate_caches
from importlib.util import module_from_spec, spec_from_file_location

from zavod.meta import Dataset

MODULE_RE = re.compile(r"^[\w\.]+:[\w]+")


def load_entry_point(dataset: Dataset, method: str = "crawl") -> Callable[[Any], None]:
    """Load the actual runner code behind the dataset. This will work either
    by specifying a file name relative to the dataset.base_path, or a proper
    Python module name."""
    invalidate_caches()
    if dataset.entry_point is None:
        raise RuntimeError("The dataset has no entry point!")
    module_name = dataset.entry_point
    if ":" in module_name:
        module_name, method = module_name.rsplit(":", 1)
    module: Optional[ModuleType] = None
    try:
        module = import_module(module_name)
    except ModuleNotFoundError:
        pass
    if module is None:
        for file_name in (module_name, f"{module_name}.py"):
            file_path = Path(file_name)
            if dataset.base_path is not None:
                file_path = dataset.base_path.joinpath(file_path)
            if file_path.is_file():
                name = f"_dataset_mod_{file_path.stem}"
                spec = spec_from_file_location(name, file_path)
                if spec is not None and spec.loader is not None:
                    module = module_from_spec(spec)
                    spec.loader.exec_module(module)
                    break
    if module is None:
        raise RuntimeError("Could not load entry point: %s" % dataset.entry_point)
    try:
        method_ = getattr(module, method)
        return cast(Callable[[Any], None], method_)
    except AttributeError:
        raise RuntimeError("Function does not exist: %r (on %r)" % (method, module))


def example_function() -> None:
    pass
