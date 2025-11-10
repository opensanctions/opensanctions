import random
from pathlib import Path
from typing import Any, Dict, List

import yaml
from normality import slugify
from zavod.settings import OPENAI_API_KEY
from zavod.shed.names.dspy.single_entity import FIELDS, load_data
from zavod.shed.names.dspy.split import init_module
from zavod.shed.names.split import SINGLE_ENTITY_PROGRAM_PATH

import dspy  # type: ignore  # type: ignore

FIELDS = ["full_name", "alias", "weak_alias", "previous_name"]
EXAMPLES_PATH = Path(__file__).parent / "multi_entity_examples.yml"


class SingleEntity(BaseModel):
    full_name: List[str] = []
    alias: List[str] = []
    weak_alias: List[str] = []
    previous_name: List[str] = []


class MultiEntitySplitSignature(dspy.Signature):  # type: ignore
    """Multiple entities extracted from a single string."""

    string: str = dspy.InputField()
    entities: List[SingleEntity] = dspy.OutputField(
        desc="A list of entities extracted from the input string."
    )
