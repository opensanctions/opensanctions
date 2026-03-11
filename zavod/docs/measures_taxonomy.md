# Sanctions measures taxonomy

Most sanctions programmes in OpenSanctions are tagged with one or more **measures** describing the types of restrictive action they impose. Measures are referenced from [programme metadata](metadata.md) via the `measures` field, and are surfaced in the UI and the [programmes JSON](https://data.opensanctions.org/meta/programs.json).

## Measures

### Aid suspension

Suspension or reduction of foreign aid, development funding, or multilateral lending to a country or territory.

### Arms embargo

Blanket prohibition on arms/military equipment to a country or regime. Country-wide scope only. If targeted at specific entities, use [Arms restrictions](#arms-restrictions) instead.

### Arms restrictions

Prohibitions on the supply, sale, transfer, or procurement of arms, ammunition, military equipment, and related services (technical assistance, training, financing, brokering). Scope and direction defined by the applicable regime.

### Asset freeze

Freezing of funds, financial assets, and economic resources owned or controlled by a designated person or entity, and the prohibition on making funds or economic resources available to or for their benefit. Includes US-style blocking (OFAC SDN). Does not cover broader financial services or market access restrictions — those fall under [Financial restrictions](#financial-restrictions).

### Debarment

Exclusion from government procurement, contracts, or programmes (e.g. Medicaid/Medicare exclusions, World Bank debarment, SAM.gov).

### Export control

Restrictions on the export, re-export, or transfer of dual-use goods, technology, software, or luxury goods to specified destinations, end-users, or end-uses. Covers outright bans and licensing requirements. Military-list items fall under [Arms restrictions](#arms-restrictions).

### Financial restrictions

Restrictions on financial transactions, services, or access beyond a designated-person asset freeze. Systemic: SWIFT disconnection, correspondent banking bans, capital-market access bans, sovereign debt restrictions, deposit caps. Entity-level: transaction-processing bans, financing/lending prohibitions, insurance/reinsurance bans, securities dealing restrictions.

### Import restrictions

Prohibitions on importing goods originating in or consigned from a sanctioned country, territory, or sector. Covers commodity-specific bans (oil, coal, gold, diamonds, charcoal, seafood, textiles, etc.) and origin-based prohibitions (e.g. forced-labour import bans).

### Investment ban

Prohibition on new investment (equity, joint ventures, capital contributions, acquisition of ownership interests) in a sanctioned country, territory, sector, or entity. Targets future capital formation, not existing assets ([Asset freeze](#asset-freeze)) or lending ([Financial restrictions](#financial-restrictions)).

### Services ban

Prohibition on providing professional, technical, or advisory services (legal, accounting, auditing, IT, consulting, engineering, advertising, trust/company formation) to sanctioned countries or persons. Where services are ancillary to a controlled-goods transfer, classify under the applicable goods category.

### Prohibition to satisfy claims

Prohibition on satisfying claims by designated persons or the government of a sanctioned country, through litigation, arbitration, or otherwise, in connection with contracts or transactions affected by sanctions.

### Sectoral sanctions

Restrictions targeting an entire economic sector (energy, defence, extractives, technology). Use when the measure does not reduce to a single more specific category above. Prefer the specific category where applicable.

### Transportation restrictions

Port access bans, airspace closures, overflight prohibitions, ship-to-ship transfer bans, vessel deflagging, aircraft landing prohibitions, and prohibitions on maritime/aviation services (crewing, classification, bunkering, insurance of vessels/aircraft).

### Travel ban

Prohibition on entry into or transit through the territory of the sanctioning jurisdiction. Designated natural persons only.

## Classification guidance

### Asset freeze vs Financial restrictions

An asset freeze follows the **person** — it freezes what they own and prohibits making funds available to them. Financial restrictions follow the **activity** — they prohibit categories of transactions regardless of individual designation. The "making available" limb (prohibiting provision of funds to a designated person) is part of the asset freeze, not a separate financial restriction.

### Arms restrictions vs Export control

Arms restrictions cover items on military lists (munitions, weapons systems, military equipment). Export control covers dual-use goods, technology, and luxury goods. Where a regime restricts both, tag both. Internal repression equipment (surveillance tech, crowd control gear) is Export control, not Arms restrictions.

### Services ancillary to goods

Where a services prohibition exists only as a component of a goods restriction (e.g. technical assistance for arms as part of the arms embargo), classify under the goods category. Use Services ban for standalone prohibitions on professional/advisory services with no physical-goods component.

### Sectoral sanctions as residual

If a sectoral measure can be fully described as an export control, import restriction, investment ban, or financial restriction, use the specific category. Sectoral sanctions is for measures defined at sector level that cannot be decomposed without loss of meaning.

### Import restrictions scope

Import restrictions covers any prohibition on bringing goods into the sanctioning jurisdiction from a target. This includes commodity embargoes (e.g. EU oil import ban on Russia), resource-origin bans (Kimberley Process), and forced-labour import prohibitions (US UFLPA, Section 307 Tariff Act).
