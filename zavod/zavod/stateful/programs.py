import functools
import yaml
from typing import Literal, Optional
from pydantic import BaseModel, Field

from zavod import settings

Measure = Literal[
    "Arms embargo",
    "Arms export restrictions",
    "Asset freeze",
    "Export control",
    "Financial block",
    "Investment ban",
    "Prohibition to satisfy claims",
    "Restrictions on goods",
    "Travel ban",
]


class Issuer(BaseModel):
    """An organization or governmental body that issues sanctions programs."""

    id: int  # from Directus, drop after migration
    name: str = Field(
        description="Name of the organization (e.g., 'UN Security Council', 'Office of Foreign Asset Control')"
    )
    acronym: Optional[str] = Field(
        default=None, description="Abbreviation (e.g., 'DFAT', 'UNSC', 'OFAC')"
    )
    organisation: Optional[str] = Field(
        default=None,
        description="Parent organization (e.g., 'United Nations', 'Government of Australia')",
    )
    territory: Optional[str] = Field(
        default=None,
        description="ISO alpha-2 country code (e.g., 'au', 'us', 'ae'), null for international bodies",
    )


class Program(BaseModel):
    """A sanctions regime."""

    id: int  # from Directus, drop after migration
    key: str = Field(
        description="Hyphenated reference key (e.g., 'AU-AFGHANISTAN', 'AE-UNSC1373', 'US-AFGH')"
    )
    title: str = Field(
        description="Title of the regime (e.g., 'Afghanistan Sanctions Framework', 'Serious Corruption Sanctions Regime')"
    )
    url: Optional[str] = Field(
        default=None,
        description="URL to program documentation or designated persons list",
    )
    summary: Optional[str] = Field(
        default=None,
        description="Purpose, legal basis, and scope of the program",
    )
    dataset: Optional[str] = Field(
        default=None,
        description="Dataset with entities from this program (e.g., 'au_dfat_sanctions', 'ae_local_terrorists')",
    )
    issuer: Optional[Issuer] = Field(
        default=None, description="Organization that administers this program"
    )
    aliases: list[str] = Field(
        default_factory=list,
        description="Alternative names or references (e.g., 'Resolution 1373', 'EO 13818')",
    )
    target_territories: list[str] = Field(
        default_factory=list,
        description="Territory codes targeted by this program (e.g., 'af', 'ru', 'by')",
    )
    measures: list[Measure] = Field(
        default_factory=list,
        description="Types of sanctions imposed (e.g., 'Asset freeze', 'Travel ban', 'Arms embargo')",
    )


@functools.cache
def _load_issuers() -> dict[str, Issuer]:
    """Load all issuers from YAML files, indexed by filename (without extension)."""
    return {
        path.stem: Issuer(**yaml.safe_load(path.read_text()))
        for path in (settings.META_RESOURCE_PATH / "issuers").glob("*.yml")
    }


# Since we'll only ever have a few programs, it's cheaper to just read them all once.
@functools.cache
def get_all_programs_by_key() -> dict[str, Program]:
    import rigour.territories

    issuers = _load_issuers()

    programs: list[Program] = []
    for path in (settings.META_RESOURCE_PATH / "programs").glob("*.yml"):
        data = yaml.safe_load(path.read_text())
        if not data:
            continue

        # Derive the program key from the filename stem to ensure they always match.
        key_from_stem = path.stem
        if "key" in data:
            assert data["key"] == key_from_stem, (
                f"Program key '{data['key']}' in {path.name} does not match "
                f"the expected key '{key_from_stem}' derived from the filename"
            )
        data["key"] = key_from_stem

        # Replace issuer reference with actual Issuer object
        issuer_key = data.get("issuer")
        if issuer_key and issuer_key in issuers:
            data["issuer"] = issuers[issuer_key].model_dump()
        else:
            data["issuer"] = None

        program = Program(**data)

        # Validate all territory codes against rigour
        # This will make the unit test fail if any don't validate
        for code in program.target_territories:
            assert rigour.territories.get_territory(code) is not None, (
                f"Unknown territory code '{code}' in program '{program.key}'"
            )
        if program.issuer and program.issuer.territory:
            assert (
                rigour.territories.get_territory(program.issuer.territory) is not None
            ), (
                f"Unknown issuer territory '{program.issuer.territory}' in program '{program.key}'"
            )

        programs.append(program)

    by_key = {p.key: p for p in programs}
    assert len(by_key) == len(programs), "Duplicate program keys detected"
    return by_key


def get_program_by_key(program_key: str) -> Optional[Program]:
    return get_all_programs_by_key().get(program_key, None)
