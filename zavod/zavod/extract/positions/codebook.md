# Position classification codebook

Classify a position label by government level, function, and seniority, using the title and its organization context.

These rules apply to human review and automated classification. They describe
the office, independently of the person who holds it. PEP status, retention,
and tenure dates belong to separate decisions.

## Evidence and scope

- Use the complete title, organization, jurisdiction, and dataset context.
  Keep the original title available when a translation needs review.
- Classify each distinct title within its context once. Apply that decision to
  equivalent instances. A shared word does not establish an equivalent role.
- Distinguish public bodies, state enterprises, intergovernmental organizations,
  political parties, and religious leadership from private organizations.
- A confirmed private or unrelated role is **out of scope**. An unclear role is
  **undecided**. Neither outcome is a seniority band.
- Resolve each dimension separately. If evidence is missing or contradictory,
  leave that dimension undecided and record the missing evidence.
- Ignore `former`, `acting`, and `interim` when assigning these attributes.
  These modifiers describe tenure or status. Keep rank modifiers such as `deputy`.
- Separate distinct offices in a compound label where possible. If a position
  spans functions or levels, select the category with the greatest influence.
  If the evidence does not establish that category, leave the decision undecided.
  For inseparable offices, use the highest supported seniority band.

## Level describes the governing jurisdiction

Assign one level when the evidence establishes a governing jurisdiction.

| Level | Assignment rule | Examples |
| --- | --- | --- |
| `gov.national` | The office belongs to central government or a body accountable to it. | National parliament; national ministry; central bank. |
| `gov.state` | The office belongs to a regional government below national government. | State cabinet; provincial assembly; regional court. |
| `gov.muni` | The office belongs to local government. | Municipal council; mayor; local executive committee. |
| `gov.igo` | The office belongs to an organization constituted by governments. | UN leadership; European Parliament; IMF governance. |

Use the institution's authority to establish level. Office location,
constituency, and geographic words alone do not establish level.
A national MP retains `gov.national` when the title names a local constituency.
A regional office of a national ministry also retains `gov.national`.

For an enterprise or public entity, use the government level to which it is
accountable. For an ambassador, use the sending government when established.
A posting abroad does not make an office `gov.igo`.

Diplomatic, party, and religious functions do not require a level in the
methodology. Leave level unassigned when no government level applies.
Use undecided when a government level applies but the evidence does not identify it.

## Function describes the office's work

Assign the most specific supported function. Seniority does not determine function.

| Function | Assignment rule | Examples |
| --- | --- | --- |
| `gov.head` | National head of state or government. | President of a country; prime minister; monarch. |
| `gov.executive` | Political direction of government, including regional and local government heads. | Minister; deputy minister; political secretary of state; governor; mayor. |
| `gov.legislative` | Membership or political leadership of a legislature. | MP; senator; regional assembly member; city councillor. |
| `gov.judicial` | Exercise or leadership of judicial authority. | Supreme court justice; constitutional court member; regional judge. |
| `gov.admin` | Civil service and administrative work in government. | Permanent secretary; ministry division head; administrative officer. |
| `gov.security` | Military, intelligence, or security service duties. | General; admiral; intelligence agency head. |
| `gov.financial` | Central banking, financial regulation, or financial governance of an IGO. | Central bank governor; financial regulator board member; IMF Executive Board member. |
| `gov.soe` | Governance or management of a state enterprise, public entity, or agency. | Board member; chief executive; corporate manager. |
| `role.diplo` | Diplomatic representation. | Ambassador; high commissioner; permanent representative; diplomatic attaché. |
| `pol.party` | An office within a political party. | Party leader; deputy leader; secretary-general. |
| `gov.religion` | Religious leadership with significant political influence. | Leader of a religious body with a documented political role. |

Apply these boundaries:

- `gov.head` describes a national constitutional office. Regional premiers and
  mayors use `gov.executive`. Heading a department does not imply `gov.head`.
- Distinguish political executive appointments from civil service appointments.
  A secretary of state needs context to distinguish `gov.executive` from `gov.admin`.
- Use the specialized function for a public body's work when it applies.
  A central bank governor uses `gov.financial`, not the general `gov.soe` category.
- Use `gov.admin` for civil service ranks and internal administrative units.
  Use `gov.soe` for governance or corporate management of a separate public entity.
- Classify support staff by their own duties. Employment in a parliament or
  court does not by itself imply `gov.legislative` or `gov.judicial`.
- A finance minister uses `gov.executive`. A finance department in an enterprise
  does not imply central banking or financial regulation.
- A party membership, family relationship, or occupation alone does not establish
  an office. Religious employment alone does not establish political influence.

## Seniority describes the rank in the title

Assign one band in this order: `leadership` > `senior` > `other`.
Use the rank that the title denotes. Organization context explains the title;
organization size, prestige, budget, and perceived risk do not change the band.
A minister has the same band across ministries and government levels.

| Band | Assignment rule | Rank anchors |
| --- | --- | --- |
| `leadership` | The title denotes leadership of a whole body, ministry, government, or its governing board. | Minister; chief executive; director-general of an agency; board chair. |
| `senior` | The title denotes a deputy leader, division manager, senior professional, or substantive member of a legislature or governing board. | Deputy minister; head of division; senior manager; MP; board member. |
| `other` | The title denotes a supporting, junior, or routine role below those ranks. | Clerk; administrative assistant; junior officer. |

Apply these rank boundaries:

- `Head of ministry` and `head of agency` denote `leadership`.
  `Head of division`, `head of section`, and `head of unit` denote `senior`.
- `Deputy minister` denotes `senior`. A deputy or assistant title does not inherit
  the principal office's band. Establish the modified rank from context.
- A civil service `Assistant Director` denotes `senior` when context confirms a
  management rank. `Assistant to the Director` denotes `other` for a support role.
- `Chief`, `director`, `president`, and `secretary` need the governed body or rank.
  `Chief clerk` does not denote leadership of an organization.
- `Senior` in a title needs evidence of a senior professional or management rank.
  It does not automatically make a routine support role `senior`.
- A `gov.head` position always has `leadership` seniority.
  An unsupported rank stays undecided; `other` is not a fallback for missing evidence.

### Seniority anchors by function

These examples assume the organization context establishes the stated office.
An em dash means no anchor is specified for that band.
It does not authorize a guess or exclude a documented rank.

| Function | `leadership` | `senior` | `other` |
| --- | --- | --- | --- |
| `gov.head` | National president; prime minister | — | — |
| `gov.executive` | Minister; regional premier; mayor | Deputy minister; deputy mayor | — |
| `gov.legislative` | Speaker of parliament; council chair | MP; senator; councillor | — |
| `gov.judicial` | Chief justice; court president | Judge; supreme court justice | — |
| `gov.admin` | Head of civil service; permanent secretary | Head of division; assistant director; senior manager | Clerk; administrative assistant |
| `gov.security` | Chief of defence; intelligence agency head | General; admiral; deputy agency head | Junior military officer |
| `gov.financial` | Central bank governor; regulator chair | Deputy governor; governing board member | Junior regulatory officer |
| `gov.soe` | Chief executive; board chair | Board member; division director; senior manager | Junior manager |
| `role.diplo` | Ambassador; high commissioner; permanent representative | Deputy head of mission | Junior diplomatic attaché |
| `pol.party` | Party leader; party secretary-general | Deputy party leader | — |
| `gov.religion` | Head of a religious body with political influence | Deputy religious leader with political influence | — |
