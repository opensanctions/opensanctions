import sys
from types import ModuleType
from typing import Callable, Any, Optional, cast
from importlib import import_module, invalidate_caches
from importlib.util import module_from_spec, spec_from_file_location

from zavod.meta import Dataset


def load_entry_point(dataset: Dataset) -> Callable[[Any], None]:
    """Load the actual runner code behind the dataset. This will work either
    by specifying a file name relative to the dataset.base_path, or a proper
    Python module name."""
    invalidate_caches()
    if dataset.module_name is None:
        raise RuntimeError("The dataset has no entry point!")
    module: Optional[ModuleType] = None
    try:
        module = import_module(dataset.module_name)
    except ModuleNotFoundError:
        pass
    if module is None:
        if dataset.module_path is not None:
            name = f"_dataset_mod_{dataset.module_path.stem}"
            spec = spec_from_file_location(name, dataset.module_path)
            if spec is not None and spec.loader is not None:
                module = module_from_spec(spec)
                sys.modules[name] = module
                spec.loader.exec_module(module)
    if module is None:
        raise RuntimeError("Could not load entry point: %s" % dataset.model.entry_point)
    try:
        method_ = getattr(module, dataset.method_name)
        return cast(Callable[[Any], None], method_)
    except AttributeError:
        raise RuntimeError(
            "Function does not exist: %r (on %r)" % (dataset.method_name, module)
        )


def example_function() -> None:
    """For unit tests."""
    pass
