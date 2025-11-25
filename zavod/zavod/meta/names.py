from functools import cached_property
from typing import Any, Dict, Optional, Set
from logging import getLogger

from followthemoney import Model
from followthemoney.schema import Schema
from pydantic import BaseModel, RootModel


log = getLogger(__name__)


class CleaningSpec(BaseModel):
    reject_chars_baseline: str = ""
    """
    The standard characters that suggest a name needs cleaning.
    """
    reject_chars: str = ""
    """
    Additional characters specific to this schema that suggest a name needs cleaning.

    Use this to define characters in dataset-specific config. Adds to the baseline
    characters for default specs.
    """
    allow_chars: str = ""
    """
    Characters that would otherwise trigger cleaning but are allowed for this schema.

    Remember that characters defined for other matching schema specs will still apply.
    """
    min_chars: int = 2
    require_space: bool = False
    allow_nullwords: bool = False

    @cached_property
    def reject_chars_consolidated(self) -> Set[str]:
        """Get the full set of characters to reject for this spec."""
        baseline = set(self.reject_chars_baseline)
        reject_extra = set(self.reject_chars)
        allow = set(self.allow_chars)
        return (baseline | reject_extra) - allow


class NamesSpec(RootModel[Dict[str, CleaningSpec]]):
    """Name cleaning requirements by schema. All matching schema configurations will apply"""

    ###################
    # NB when introducing more specific defaults, these could take prescedence
    # over the defaults defined here. Datasets extending these defaults might
    # need to be updated to extend the new default instead.
    root: Dict[str, CleaningSpec] = {
        "Person": CleaningSpec(
            reject_chars_baseline=";\\/()[]<>{}:",
            require_space=True,
        ),
        "LegalEntity": CleaningSpec(
            reject_chars_baseline="/;",
        ),
        "Vessel": CleaningSpec(
            reject_chars_baseline="/;",
        ),
    }

    @classmethod
    def model_validate(cls, obj: Any, **kwargs: Any) -> "NamesSpec":
        """Merge provided values with defaults."""
        if isinstance(obj, dict):
            instance = cls()
            for schema_name, spec in obj.items():
                if schema_name in instance.root:
                    schema = Model.instance().get(schema_name)
                    assert schema is not None, schema_name
                    # Merge with default
                    default_spec = instance.root[schema_name]
                    merged_spec = default_spec.model_copy(update=spec)
                    instance.root[schema_name] = merged_spec
                else:
                    instance.root[schema_name] = CleaningSpec.model_validate(spec)
            return instance
        raise TypeError(f"object must be a dict, got {type(obj)}")

    def get_spec(self, schema: Schema) -> Optional[CleaningSpec]:
        """Returns the spec for the most specific schema that matches the entity."""
        matching_specs = [
            (Model.instance().get(name), spec)
            for name, spec in self.root.items()
            if schema.is_a(name)
        ]
        # schema names validated in model_validate
        specs = [(s, spec) for s, spec in matching_specs if s is not None]
        specs.sort(key=lambda pair: len(pair[0].schemata), reverse=True)
        # We don't support multiple inheritance for now. Unlikely to define a spec for Asset.
        return specs[0][1] if specs else None
