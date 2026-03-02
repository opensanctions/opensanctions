from functools import cached_property
from typing import Any, Dict, Optional, Set
from logging import getLogger

from followthemoney import Model
from followthemoney.schema import Schema
from pydantic import BaseModel, ConfigDict

log = getLogger(__name__)


class CleaningSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reject_chars_baseline: str = ""
    """The standard characters that suggest a name needs cleaning."""
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
    min_length: int = 2
    """Minimum length for names. Does not apply to "dense" writing systems like Han for Chinese."""
    single_token_min_length: int = 2
    """Minimum length for names with no spaces, i.e. a single token.
    Does not apply to writing systems that don't use spaces to separate name parts, e.g. Han for Chinese"""
    require_space: bool = False
    """Whether to require a space in the name.
    Does not apply to writing systems that don't use spaces to separate name parts, e.g. Han for Chinese"""
    allow_nullwords: bool = False

    @cached_property
    def reject_chars_consolidated(self) -> Set[str]:
        """Get the full set of characters to reject for this spec."""
        baseline = set(self.reject_chars_baseline)
        reject_extra = set(self.reject_chars)
        allow = set(self.allow_chars)
        return (baseline | reject_extra) - allow


_DEFAULT_SCHEMA_RULES: Dict[str, CleaningSpec] = {
    ###################
    # Beware that when introducing defaults for more specific schemata, these could take
    # precedence over extensions to the existing defaults in some datasets' metadata.
    # Those datasets might need to be updated to extend the new default instead.
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


class NamesSpec(BaseModel):
    """Name cleaning requirements and heuristics for a dataset."""

    schema_rules: Dict[str, CleaningSpec] = dict(_DEFAULT_SCHEMA_RULES)
    """Name cleaning requirements by schema. All matching schema configurations will apply."""

    suggest_person_single_token: bool = False
    """
    If True, single-token Person names (after stripping name prefixes such as "Mr.")
    are suggested as weakAlias rather than name.
    """

    suggest_uppercase_org_single_token_shorter_than: Optional[int] = None
    """
    If set, Organization names that are all-uppercase, contain no spaces, and are
    shorter than this threshold are suggested as abbreviation rather than name.
    """

    suggest_non_person_single_token_shorter_than: Optional[int] = None
    """
    If set, LegalEntity-but-not-Person names (i.e. companies, organisations, vessels, etc.)
    that are all-uppercase, contain no spaces, and are shorter than this threshold are
    suggested as abbreviation rather than name.
    """

    @classmethod
    def model_validate(cls, obj: Any, **kwargs: Any) -> "NamesSpec":
        """Merge provided schema_rules values with defaults."""
        if isinstance(obj, dict):
            instance = cls()
            for schema_name, spec in obj.get("schema_rules", {}).items():
                if schema_name in instance.schema_rules:
                    schema = Model.instance().get(schema_name)
                    assert schema is not None, schema_name
                    # Merge with default
                    default_spec = instance.schema_rules[schema_name]
                    merged_spec = default_spec.model_copy(update=spec)
                    instance.schema_rules[schema_name] = merged_spec
                else:
                    instance.schema_rules[schema_name] = CleaningSpec.model_validate(
                        spec
                    )
            instance.suggest_person_single_token = obj.get(
                "suggest_person_single_token", False
            )
            instance.suggest_uppercase_org_single_token_shorter_than = obj.get(
                "suggest_uppercase_org_single_token_shorter_than", None
            )
            instance.suggest_non_person_single_token_shorter_than = obj.get(
                "suggest_non_person_single_token_shorter_than", None
            )
            return instance
        raise TypeError(f"object must be a dict, got {type(obj)}")

    def get_spec(self, schema: Schema) -> Optional[CleaningSpec]:
        """Returns the spec for the most specific schema that matches the entity."""
        matching_specs = [
            (Model.instance().get(name), spec)
            for name, spec in self.schema_rules.items()
            if schema.is_a(name)
        ]
        # schema names validated in model_validate
        specs = [(s, spec) for s, spec in matching_specs if s is not None]
        specs.sort(key=lambda pair: len(pair[0].schemata), reverse=True)
        # We don't support multiple inheritance for now. Unlikely to define a spec for Asset.
        return specs[0][1] if specs else None
