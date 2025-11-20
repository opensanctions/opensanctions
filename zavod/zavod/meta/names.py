from functools import cached_property
from typing import Any, Dict, List, Set

from followthemoney.schema import Schema
from pydantic import BaseModel, RootModel


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

    @cached_property
    def reject_chars_consolidated(self) -> Set[str]:
        """Get the full set of characters to reject for this spec."""
        baseline = set(self.reject_chars_baseline)
        reject_extra = set(self.reject_chars)
        allow = set(self.allow_chars)
        return (baseline | reject_extra) - allow


class NamesSpec(RootModel[Dict[str, CleaningSpec]]):
    """Name cleaning requirements by schema. All matching schema configurations will apply"""

    root: Dict[str, CleaningSpec] = {
        "Person": CleaningSpec(
            reject_chars_baseline=";\\/()[]<>{}:",
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
            for schema, spec in obj.items():
                if schema in instance.root:
                    # Merge with default
                    default_spec = instance.root[schema]
                    merged_spec = default_spec.model_copy(update=spec)
                    instance.root[schema] = merged_spec
                else:
                    instance.root[schema] = CleaningSpec.model_validate(spec)
            return instance
        raise TypeError(f"object must be a dict, got {type(obj)}")

    def specs_for_schema(self, schema: Schema) -> List[CleaningSpec]:
        """Returns the specs for all schemas that match the entity."""
        return [spec for name, spec in self.root.items() if schema.is_a(name)]
