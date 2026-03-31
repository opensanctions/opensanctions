import functools
import yaml
from typing import Literal, Optional
from pydantic import BaseModel, Field

from zavod import settings

# TODO: relatively soon we'll need to turn this into a dict if we want to
# hotwire a special prop (like Sanctions:measures) to use it.
# https://github.com/opensanctions/opensanctions/pull/3752#pullrequestreview-3924019716

Measure = Literal[
    # Suspension or reduction of foreign aid, development funding, or
    # multilateral lending to a country or territory.
    "Aid suspension",
    # Blanket prohibition on arms/military equipment to a country or regime.
    # Country-wide scope only. If targeted at specific entities, use
    # "Arms restrictions" instead.
    "Arms embargo",
    # Prohibitions on the supply, sale, transfer, or procurement of arms,
    # ammunition, military equipment, and related services (technical
    # assistance, training, financing, brokering). Scope and direction
    # defined by the applicable regime.
    "Arms restrictions",
    # Freezing of funds, financial assets, and economic resources owned or
    # controlled by a designated person or entity, and the prohibition on
    # making funds or economic resources available to or for their benefit.
    # Includes US-style blocking (OFAC SDN). Does not cover broader
    # financial services or market access restrictions (Financial restrictions).
    "Asset freeze",
    # Exclusion from government procurement, contracts, or programmes
    # (Medicaid/Medicare exclusions, World Bank debarment, SAM.gov).
    "Debarment",
    # Restrictions on the export, re-export, or transfer of dual-use goods,
    # technology, software, or luxury goods to specified destinations,
    # end-users, or end-uses. Covers outright bans and licensing
    # requirements. Military-list items fall under Arms restrictions.
    "Export control",
    # Restrictions on financial transactions, services, or access beyond
    # a designated-person asset freeze. Systemic: SWIFT disconnection,
    # correspondent banking bans, capital-market access bans, sovereign
    # debt restrictions, deposit caps. Entity-level: transaction-processing
    # bans, financing/lending prohibitions, insurance/reinsurance bans,
    # securities dealing restrictions.
    "Financial restrictions",
    # Prohibitions on importing goods originating in or consigned from a
    # sanctioned country, territory, or sector. Covers commodity-specific
    # bans (oil, coal, gold, diamonds, charcoal, seafood, textiles, etc.)
    # and origin-based prohibitions (e.g. forced-labour import bans).
    "Import restrictions",
    # Prohibition on new investment (equity, joint ventures, capital
    # contributions, acquisition of ownership interests) in a sanctioned
    # country, territory, sector, or entity. Targets future capital
    # formation, not existing assets (Asset freeze) or lending (Financial
    # restrictions).
    "Investment ban",
    # Prohibition on providing professional, technical, or advisory services
    # (legal, accounting, auditing, IT, consulting, engineering, advertising,
    # trust/company formation) to sanctioned countries or persons. Where
    # services are ancillary to a controlled-goods transfer, classify under
    # the applicable goods category.
    "Services ban",
    # Prohibition on satisfying claims by designated persons or the
    # government of a sanctioned country, through litigation, arbitration,
    # or otherwise, in connection with contracts or transactions affected
    # by sanctions.
    "Prohibition to satisfy claims",
    # Restrictions targeting an entire economic sector (energy, defence,
    # extractives, technology). Use when the measure does not reduce to a
    # single more specific category above. Prefer the specific category
    # where applicable.
    "Sectoral sanctions",
    # Port access bans, airspace closures, overflight prohibitions,
    # ship-to-ship transfer bans, vessel deflagging, aircraft landing
    # prohibitions, and prohibitions on maritime/aviation services
    # (crewing, classification, bunkering, insurance of vessels/aircraft).
    "Transportation restrictions",
    # Prohibition on entry into or transit through the territory of the
    # sanctioning jurisdiction. Designated natural persons only.
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
        description="Types of sanctions imposed (e.g., 'Asset freeze', 'Travel ban', 'Arms restrictions')",
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
