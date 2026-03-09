import functools
import yaml
from typing import Literal, Optional
from pydantic import BaseModel, Field

from zavod import settings

Measure = Literal[
    # Suspension of foreign aid, development funding, or multilateral
    # lending to a country.
    "Aid suspension",
    # Blanket prohibition on arms/military equipment to a country or regime.
    # Country-wide scope only. If targeted at specific entities, use
    # "Export control" instead.
    "Arms embargo",
    # Freezing of funds and economic resources of a designated person/entity.
    # Includes US-style "blocking" — treat these as equivalent.
    "Asset freeze",
    # Exclusion from government procurement, contracts, or programs
    # (Medicaid/Medicare exclusions, World Bank debarment, SAM.gov).
    "Debarment",
    # Restrictions on export of dual-use goods, technology, military items,
    # or luxury goods to specific destinations or end-users. Covers both
    # outright bans and licensing requirements.
    "Export control",
    # Systemic financial measures: correspondent banking bans, SWIFT cutoffs,
    # capital market access bans, sovereign debt restrictions, insurance bans.
    # NOT entity-level freezes — those go under "Asset freeze".
    "Financial restrictions",
    # Prohibitions on importing goods originating from a sanctioned country
    # or sector (oil, coal, gold, diamonds, iron/steel, etc.).
    "Import restrictions",
    # Prohibition on new investment (equity, JVs, capital contributions) in
    # a sanctioned country or sector. Targets future capital flows, not
    # existing assets.
    "Investment ban",
    # Prohibition on providing professional services (legal, accounting, IT,
    # consulting, engineering, advertising, trust formation) to sanctioned
    # countries or persons. No physical goods involved.
    "Services ban",
    # Bars sanctioned parties from claiming compensation for the effects of
    # sanctions via litigation or arbitration.
    "Prohibition to satisfy claims",
    # Broad restrictions targeting entire economic sectors (energy, defense,
    # mining, technology). Use when the measure applies at sector level and
    # doesn't reduce to a single trade or financial category above.
    "Sectoral sanctions",
    # Port access bans, airspace closures, ship-to-ship transfer bans,
    # vessel deflagging, prohibitions on maritime/aviation services
    # (crewing, classification, insurance).
    "Transportation restrictions",
    # Prohibition on entry into or transit through the sanctioning
    # jurisdiction. Natural persons only.
    "Travel ban",
]


class Issuer(BaseModel):
    """An organization or governmental body that issues sanctions programs."""

    id: Optional[int] = None  # from Directus, drop after migration
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

    id: Optional[int] = None  # from Directus, drop after migration
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
        assert data, f"Empty or invalid YAML in {path.name}"

        # Ensure the program key in the YAML matches the filename.
        assert data.get("key") == path.stem, (
            f"Program key '{data.get('key')}' in {path.name} does not match "
            f"the expected key '{path.stem}' derived from the filename"
        )

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
            assert (
                rigour.territories.get_territory(code) is not None
            ), f"Unknown territory code '{code}' in program '{program.key}'"
        if program.issuer and program.issuer.territory:
            assert (
                rigour.territories.get_territory(program.issuer.territory) is not None
            ), f"Unknown issuer territory '{program.issuer.territory}' in program '{program.key}'"

        programs.append(program)

    by_key = {p.key: p for p in programs}
    assert len(by_key) == len(programs), "Duplicate program keys detected"
    return by_key


def get_program_by_key(program_key: str) -> Optional[Program]:
    return get_all_programs_by_key().get(program_key, None)
