from typing import Any, Dict, List

from followthemoney.schema import Schema
from pydantic import BaseModel, RootModel


class CleaningSpec(BaseModel):
    dirty_chars: str = ""
    """
    The standard characters that suggest a name needs cleaning.

    Use this to override the default set of characters.
    There's no need to redefine all the characters handled by an ancestor schema;
    """
    dirty_chars_extra: str = ""
    """
    Additional characters specific to this schema that suggest a name needs cleaning.

    Use this if you don't want to override the standard characters.
    """


class NamesSpec(RootModel[Dict[str, CleaningSpec]]):
    """Name cleaning requirements by schema. All matching schema configurations will apply"""

    root: Dict[str, CleaningSpec] = {
        "Person": CleaningSpec(
            dirty_chars=";\\/()[]<>{}:",
        ),
        "LegalEntity": CleaningSpec(
            dirty_chars="/;",
        ),
        "Vessel": CleaningSpec(
            dirty_chars="/;",
        ),
    }

    @classmethod
    def model_validate(cls, obj: Any, **kwargs: Any) -> "NamesSpec":
        """Merge provided values with defaults."""
        if isinstance(obj, dict):
            instance = cls()
            for schema, spec in obj.items():
                if schema in instance:
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
