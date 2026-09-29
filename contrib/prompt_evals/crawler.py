"""Access to the crawler code under evaluation: its prompt, response model and a
zavod Context whose cache stores the model responses so re-runs are free."""

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from pydantic import BaseModel
from zavod.context import Context
from zavod.meta import Dataset, load_dataset_from_path
from zavod.runtime.loader import load_entry_point


def load_dataset(dataset_path: Path) -> Dataset:
    dataset = load_dataset_from_path(dataset_path)
    if dataset is None:
        raise RuntimeError(f"Could not load dataset from {dataset_path}")
    return dataset


def fixtures_path(dataset: Dataset) -> Path:
    """Fixtures live next to the crawler code, in `evals/cases.yml`."""
    assert dataset.base_path is not None
    return dataset.base_path / "evals" / "cases.yml"


def load_module(path: Path) -> ModuleType:
    """Import a crawler module from an arbitrary file, e.g. a candidate version
    of the crawler that is not checked out in the working tree."""
    spec = spec_from_file_location(f"_eval_mod_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CrawlerPrompt:
    """The prompt, response model and token limit a crawler passes to
    `run_typed_text_prompt`, read from module attributes of the entry point or of
    the module at `crawler_file`."""

    def __init__(
        self,
        dataset: Dataset,
        response_type: str,
        crawler_file: Path | None = None,
        prompt_attr: str = "PROMPT",
        max_tokens_attr: str = "MAX_TOKENS",
    ) -> None:
        module = load_module(crawler_file) if crawler_file is not None else None

        def attr(name: str) -> Any:
            if module is not None:
                return getattr(module, name)
            return load_entry_point(dataset, name)

        self.prompt = cast(str, attr(prompt_attr))
        self.response_type = cast(type[BaseModel], attr(response_type))
        try:
            self.max_tokens = cast(int, attr(max_tokens_attr))
        except (RuntimeError, AttributeError):
            self.max_tokens = 3000


def make_context(dataset: Dataset) -> Context:
    return Context(dataset)
