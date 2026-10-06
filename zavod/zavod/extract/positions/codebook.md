# Position classification codebook

Classify a position by government level, role, and seniority. Use the title and its organization context.

These rules apply to human review and automated classification. They describe
the office, independently of the person who holds it. PEP status, retention,
and tenure dates require separate decisions.

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
- Separate distinct offices in a compound label where possible.
  Assign each office its own level, role, and seniority.
  Do not compare seniority bands across different level/role combinations.
  Leave an inseparable combination undecided when one classification cannot represent it.

## Level describes the governing jurisdiction

Assign one level when the evidence establishes a governing jurisdiction.

- `gov.national`: Central government or a body accountable to it.
  Examples: national parliament, national ministry, central bank.
- `gov.state`: Regional government below national government.
  Examples: state cabinet, provincial assembly, regional court.
- `gov.muni`: Local government.
  Examples: municipal council, mayor, local executive committee.
- `gov.igo`: An organization constituted by governments.
  Examples: UN leadership, European Parliament, IMF governance.

Use the institution's authority to establish level. Office location,
constituency, and geographic words alone do not establish level.
A national MP retains `gov.national` when the title names a local constituency.
A regional office of a national ministry also retains `gov.national`.

For an enterprise, use the level of the government that controls it.
For another public body, use the government level to which it is accountable.
For an ambassador, use the sending government when established.
A posting abroad does not make an office `gov.igo`.

Diplomatic, party, and religious roles do not require a level in this methodology.
Leave level unassigned when no government level applies.
Use undecided when a government level applies but the evidence does not identify it.

## Role describes the office's work

Assign the most specific supported role. Seniority does not determine role.

- `gov.head`: National head of state or government.
  Examples: president of a country, prime minister, monarch.
- `gov.executive`: Political direction of government, including regional and local government heads.
  Examples: minister, deputy minister, political secretary of state, governor, mayor.
- `gov.legislative`: Membership or political leadership of a legislature.
  Examples: MP, senator, regional assembly member, city councillor.
- `gov.judicial`: Exercise or leadership of judicial authority.
  Examples: supreme court justice, constitutional court member, regional judge.
- `gov.admin`: Civil service and administrative work in government or an IGO.
  Examples: permanent secretary, ministry division head, administrative officer, IGO administrative director.
- `gov.security`: Military, intelligence, or security service duties.
  Examples: general, admiral, intelligence agency head.
- `gov.financial`: Central banking, financial regulation, or financial governance of an IGO.
  Examples: central bank governor, financial regulator board member, IMF Executive Board member.
- `gov.soe`: Governance or management of a publicly controlled entity that offers goods or services on a market.
  Examples: enterprise board member, chief executive, corporate manager.
  See the SOE definition below.
- `role.diplo`: Diplomatic representation.
  Examples: ambassador, high commissioner, permanent representative, diplomatic attaché.
- `pol.party`: An office within a political party.
  Examples: party leader, deputy leader, secretary-general.
- `gov.religion`: Religious leadership with significant political influence.
  Example: leader of a religious body with a documented political role.

Apply these boundaries:

- `gov.head` describes a national constitutional office. Regional premiers and
  mayors use `gov.executive`. Heading a department does not imply `gov.head`.
- Distinguish political executive appointments from civil service appointments.
  A secretary of state needs context to distinguish `gov.executive` from `gov.admin`.
- Use the specialized role for a public body's work when it applies.
  A central bank governor uses `gov.financial`.
- Use `gov.admin` for civil service ranks and administrative bodies.
  Separate legal status, public funding, or an agency title alone does not establish `gov.soe`.
- IGO board membership does not imply `gov.soe`.
  Use the role supported by the organization's work and the office's duties.
- Classify support staff by their own duties. Employment in a parliament or
  court does not by itself imply `gov.legislative` or `gov.judicial`.
- A finance minister uses `gov.executive`. A finance department in an enterprise
  does not imply central banking or financial regulation.
- Party membership, a family relationship, or an occupation alone does not establish
  an office. Religious employment alone does not establish political influence.

## Seniority describes rank within a level/role combination

Assign one band in this order: `leadership` > `senior` > `middle` > `junior`.
Compare offices within the same level/role combination. Do not rank a person
against everyone in the organization or across government.

Organization context establishes the office and its authority within that combination.
Organization size, prestige, budget, and perceived risk do not determine the band.
Leading a team or holding the highest rank in one ministry does not automatically imply `leadership`.

- `leadership`: An office that directs the governing authority or role as a whole,
  as established by the role anchors below.
  Examples: national head of government, head of civil service, chief of defence.
- `senior`: An upper rank with substantial authority or responsibility within the combination.
  Examples: minister, senior ministry official, supreme court justice, enterprise board member.
- `middle`: An intermediate rank below the senior offices within the combination.
  Examples: deputy minister, ministry division head, enterprise division manager.
- `junior`: A lower rank with limited authority or supporting duties within the combination.
  Examples: junior officer, clerk, administrative assistant.

For a national ministry:

- Minister: level `gov.national`, role `gov.executive`, seniority `senior`.
- Deputy minister: level `gov.national`, role `gov.executive`, seniority `middle`.
- Senior government official: level `gov.national`, role `gov.admin`, seniority `senior`,
  when the evidence confirms substantial administrative authority.

The senior government official can report to the deputy minister.
Their bands describe rank within different roles. The `senior` band does not
place the administrative official above the `middle` executive official.

Apply these rank boundaries:

- Establish the scope of `head`, `chief`, `director`, `president`, and `secretary`.
  These words alone do not establish a band.
- A deputy or assistant title does not inherit the principal office's band.
  Establish the modified rank from context and the role anchors.
- A civil service `Assistant Director` can denote `middle` when context confirms a management rank.
  `Assistant to the Director` denotes `junior` when it describes a support role.
- `Senior` in a title needs evidence of a senior professional or management rank.
  It does not automatically make a routine support role `senior`.
- A `gov.head` position always has `leadership` seniority.
- An unsupported rank stays undecided. `middle` and `junior` are not substitutes for missing evidence.

### Seniority anchors by role

These examples assume the context establishes the stated office and its authority.
Apply each anchor within its government level. A missing band has no default example.
It does not exclude a documented rank.

- `gov.head`:
  - a) `leadership`: national president, prime minister, monarch.
- `gov.executive`:
  - a) `leadership`: regional premier, municipal mayor.
  - b) `senior`: minister, member of a regional or municipal executive with comparable responsibility.
  - c) `middle`: deputy minister, deputy mayor with a subordinate executive remit.
- `gov.legislative`:
  - a) `leadership`: speaker of parliament, assembly speaker, council chair.
  - b) `senior`: MP, senator, regional assembly member, councillor.
- `gov.judicial`:
  - a) `leadership`: chief justice or equivalent head of the judiciary at that level.
  - b) `senior`: supreme court justice, constitutional court member, senior court president.
  - c) `middle`: judge of a lower court with a subordinate judicial remit.
- `gov.admin`:
  - a) `leadership`: head of civil service, head of an IGO's general administration.
  - b) `senior`: permanent secretary, agency director-general, senior ministry official,
    senior IGO administrative director, deputy head of IGO administration, administrative board member.
  - c) `middle`: ministry division head, section head, unit head, assistant director with a management rank.
  - d) `junior`: clerk, administrative assistant, junior administrative officer.
- `gov.security`:
  - a) `leadership`: chief of defence, intelligence agency head.
  - b) `senior`: general, admiral, deputy intelligence agency head.
  - c) `middle`: military officer with an intermediate command rank.
  - d) `junior`: junior military officer.
- `gov.financial`:
  - a) `leadership`: central bank governor, financial regulator chair.
  - b) `senior`: deputy governor, governing board member, IMF Executive Board member.
  - c) `middle`: regulatory division manager.
  - d) `junior`: junior regulatory officer.
- `gov.soe`:
  - a) `leadership`: enterprise chief executive, board chair.
  - b) `senior`: governing board member, senior executive with enterprise-wide responsibility.
  - c) `middle`: division director, operational manager.
  - d) `junior`: junior manager with limited authority.
- `role.diplo`:
  - a) `leadership`: ambassador, high commissioner, permanent representative.
  - b) `senior`: deputy head of mission.
  - c) `middle`: diplomatic attaché with an established intermediate rank.
  - d) `junior`: junior diplomatic attaché.
- `pol.party`:
  - a) `leadership`: party leader, party secretary-general with authority over the party.
  - b) `senior`: deputy party leader, senior party official.
  - c) `middle`: party official with an intermediate administrative rank.
  - d) `junior`: party official with limited supporting duties.
- `gov.religion`:
  - a) `leadership`: head of a religious body with documented political influence.
  - b) `senior`: deputy religious leader with documented political influence.

## Examples explicitly named in the FATF guidance

The source is [FATF Guidance on Politically Exposed Persons, June 2013](https://www.fatf-gafi.org/content/dam/fatf-gafi/guidance/Guidance-PEP-Rec12-22.pdf).
Page references use the printed page numbers.

Named positions in paragraph 40, page 11:

- President: `gov.national`, `gov.head`, `leadership`, when the title denotes the national head of state.
- Minister: `gov.national`, `gov.executive`, `senior`, for a national political appointment.
- Deputy minister: `gov.national`, `gov.executive`, `middle`, for the subordinate national executive office.

IGO positions in paragraphs 11 and 36, pages 5 and 10:

- Director: level `gov.igo`. For a senior administrative director, use `gov.admin` and `senior`.
  Use `leadership` when the office heads the IGO's general administration.
- Deputy director: level `gov.igo`. A deputy in senior administrative management uses `gov.admin` and `senior`.
- Board member or an equivalent office: level `gov.igo`.
  Use `gov.admin` and `senior` for a general administrative board.
  Use `gov.financial` and `senior` for an IMF Executive Board member.

These IGO titles require evidence of senior management duties.
An internal department title alone does not establish that rank.

Broader role categories in paragraphs 11 and 36, pages 4–5 and 10:

- Heads of state or government: `gov.head`, `leadership`.
- Senior politicians: resolve the office to `gov.executive`, `gov.legislative`, or `pol.party` from context.
- Senior government officials: `gov.admin`, `senior`, when the duties are administrative.
- Senior judicial officials: `gov.judicial`, `senior` or `leadership`, according to the office.
- Senior military officials: `gov.security`, `senior` or `leadership`, according to the office.
- Senior executives of state corporations: `gov.soe`, `senior` or `leadership`, according to the office.
- Important party officials: `pol.party`, with seniority established from the office's authority.

The guidance also names decision powers in paragraph 41, page 11:

- Final approval of government procurement.
- Responsibility for spending above a specified amount.
- Decisions on government subsidies and grants.

These powers help establish authority. They do not identify the role or level by themselves.

Paragraph 37 excludes intermediate and junior officials from FATF's PEP definition.
Paragraph 38 leaves the precise PEP threshold to context.
Paragraph 40 explicitly includes deputy ministers among possible PEP positions.
The bands in this codebook describe rank within a level/role combination.
They do not reproduce FATF's PEP threshold. A `middle` classification does not determine PEP status.

## SOE definition

A state-owned enterprise (SOE) offers goods or services on a market and is subject to public dominant influence.
Establish both requirements before assigning `gov.soe`:

- **Economic activity:** The entity offers goods or services on a market.
  Legal form, funding method, and a profit objective do not determine whether its activity is economic.
  Public powers alone do not establish economic activity.
  Source: [Commission Notice 2016/C 262/01, paragraphs 7–12 and 17–18](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52016XC0719(05)).
- **Public control:** Public authorities exercise dominant influence directly or indirectly through ownership, financial participation, or governing rules.
  National, regional, and local authorities all qualify.
  Source: [Directive 2006/111/EC, Article 2(a)–(b)](https://eur-lex.europa.eu/eli/dir/2006/111/oj/eng).

Public dominant influence is presumed when the authorities directly or indirectly have any of these rights:

- A majority of the enterprise's subscribed capital.
- Control of the majority of votes attached to its shares.
- The right to appoint more than half the members of an administrative, management, or supervisory body.

These presumptions do not exclude other documented dominant influence.
Source: [Directive 2006/111/EC, Article 2(b)](https://eur-lex.europa.eu/eli/dir/2006/111/oj/eng).

Apply these boundaries:

- A public minority holding alone does not establish control.
- Public funding or regulation alone does not establish control.
- Municipal and regional enterprises can use `gov.soe`.
  Assign their government level separately.
- For entities with mixed economic and public-authority activities, establish which activity the office governs.
- Record the evidence for economic activity, public control, and the office's duties.
  Leave the enterprise boundary undecided when the evidence cannot establish it.

Examples:

- A municipal water enterprise selling services under municipal control qualifies for `gov.soe`.
- A ministry's licensing agency uses `gov.admin` when it only exercises public powers.

### Enterprise classification and EU PEP scope

The EU AML Regulation generally applies from 10 July 2027.
It limits regional and local enterprise coverage to medium or large enterprises or groups.
Those size limits govern its PEP scope. They do not define the role in this codebook.
Sources: [Regulation (EU) 2024/1624, Articles 2(1)(34)(a)(vii) and 90](https://eur-lex.europa.eu/eli/reg/2024/1624/oj/eng).
