from datetime import datetime
import os
from typing import Optional, Set
from pydantic import BaseModel, Field, field_validator

from normality import slugify
from followthemoney import registry
from followthemoney.dataset.dataset import DatasetModel as FTMDatasetModel
from followthemoney.dataset.util import Url
from zavod import settings


class DataModel(BaseModel):
    url: Url
    mode: Optional[str] = None
    format: Optional[str] = None
    api_key: Optional[str] = Field(None, exclude=True)
    lang: Optional[str] = Field(None, exclude=True)

    @field_validator("api_key", mode="before")
    @classmethod
    def expand_api_key(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            return os.path.expandvars(value)
        return value

    @field_validator("lang", mode="after")
    @classmethod
    def validate_lang(cls, value: str) -> str:
        if registry.language.clean_text(value) is None:
            raise ValueError(f"Invalid language code: {value!r}")
        return value


class ZavodDatasetModel(FTMDatasetModel):
    entry_point: Optional[str] = None
    """Code location for the crawler script"""

    prefix: Optional[str] = None
    """A prefix for the dataset, used to generate entity IDs."""

    disabled: bool = False
    """Do not update the crawler at the moment."""

    hidden: bool = False
    """Do not show this dataset in the website and other UIs."""

    resolve: bool = Field(True, exclude=True)
    """Resolve entities in this dataset to canonical IDs."""

    updated_at: Optional[datetime] = Field(default_factory=lambda: settings.RUN_TIME)

    exports: Set[str] = Field(set(), exclude=True)
    """Names of all the exporters enabled for this dataset."""

    ci_test: bool = Field(True, exclude=True)
    """Whether this dataset should be automatically run in CI environments."""

    load_statements: bool = Field(False, exclude=True)
    """Whether this dataset should be loaded into the database."""

    full_dataset: Optional[str] = None
    """The name of the full dataset that this dataset is derived from, if any."""

    data: Optional[DataModel] = None
    """Data source specification."""

    summary: Optional[str] = Field(
        default=None,
        description="A short summary of the dataset, used in the website and other UIs.",
        min_length=50,
    )

    @field_validator("prefix", mode="after")
    @classmethod
    def check_prefix(cls, prefix: Optional[str]) -> Optional[str]:
        if prefix is None:
            return None
        if prefix != slugify(prefix, sep="-"):
            msg = f"Dataset prefix is invalid: {prefix!r}. Must be a slugified string"
            raise ValueError(msg)
        prefix = prefix.strip("-")
        if len(prefix) == 0:
            raise ValueError("Dataset prefix cannot be empty")
        return prefix
