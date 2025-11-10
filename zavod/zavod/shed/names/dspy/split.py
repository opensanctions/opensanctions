from functools import cache
from typing import List

from pydantic import BaseModel
from zavod.settings import OPENAI_API_KEY
from zavod.shed.names.split import (
    LLM_MODEL_VERSION,
    MULTI_ENTITY_PROGRAM_PATH,
    SINGLE_ENTITY_PROGRAM_PATH,
)

import dspy  # type: ignore
