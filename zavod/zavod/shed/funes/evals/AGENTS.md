# Evaluation guidance

These evals define the ground truth used to judge the funes extraction
pipeline. They must remain an independent check on the runtime prompts: do not
treat current prompt wording as proof that an expectation is correct, and do not
change an expectation merely to make the current model pass.

Keep this file focused on fixture-authoring and evaluation decisions. Do not copy
runtime prompts here. When a prompt and fixture disagree, review the captured
source, runtime brief, and intended product semantics before changing either one.

## Fixture ground truth

- A case consists of a frozen capture plus a trusted, brief-scoped request. The
  brief selects relevant relationships but does not itself establish page facts.
- Include every source-supported, in-scope person-position holding, including
  historical, future, former, or incidental mentions. Merge duplicate mentions
  with an identical source-form name, but do not reconcile spelling variants,
  aliases, or likely typos; entity resolution belongs to Zavod. Retain multiple
  explicitly stated holdings and distinct terms of the same office.
- Preserve source wording exactly for evaluated fields, including capitalization,
  punctuation, language, displayed honorifics, and apparent source errors.
  Downstream normalization and interpretation belong to Zavod.
- Document titles and meta descriptions are valid source evidence.
- An organization explicitly scoping its own staff or positions may supply the
  organization for entries beneath it. A named committee, council, chamber, or
  other sub-body belongs in the position name while the organization remains the
  explicitly stated parent. Do not apply a page organization when the page
  aggregates multiple organizations, such as unnamed partner representatives.
- `jurisdiction` is a source-stated geographic area associated with a holding. It
  may be an office's geographic scope, a constituency, or a geographic home
  authority. An organization, employer, or institution name is not a
  jurisdiction.
- Extract countries only from unambiguous nationality or citizenship statements.
  Origin, residence-like wording, nearby geographic labels, and demonym
  adjectives are insufficient.
- Date fields contain date expressions, not event-relative prose. In an
  appointment or term roster, a lone year attached to a holding may be treated as
  its start date and a stated range supplies start and end dates. Preserve an
  apparent typo when the source still presents it as a date.
- URLs, link targets, filenames, and resource paths may guide evidence discovery
  but cannot alone establish a person-position fact.

Outcome classification and evidence boundaries are implemented by the extraction
contract in `zavod/zavod/shed/funes/extract.py`. Blocker fixtures are intentional
regression tests of the repair-versus-terminal-miss boundary, even when they
closely resemble an explicit runtime rule. Ambiguous access denials lean broken
so a future repair workflow can retry them. A redirect is not itself an outcome:
judge its destination as usable content, a terminal miss, or a repairable
blocker.

## Extraction evaluation contract

- `Hit` expectations require an exact, order-independent person-position graph.
  Names, dates of birth, countries, position names, organizations, jurisdictions,
  start dates, end dates, populated values, and nulls must all agree.
- `Person.bio` and `Position.description` are informational and are not part of
  exact matching because multiple verbatim excerpts may be equally valid.
- Miss and broken expectations assert the result kind only. Their reason text is
  useful ground-truth documentation, but semantically equivalent prose is too
  variable to compare reliably.
- F1 and per-field accuracy scores are diagnostics. They help explain a failed
  exact hit assertion; they are not substitutes for it.

## Maintaining fixtures

- Prefer small captures that isolate one behavior, while retaining enough page
  context to make the expected interpretation defensible.
- Fixtures inspired by live pages preserve only the abstract structure needed to
  reproduce a behavior: DOM hierarchy, ordering, evidence relationships, noise,
  and approximate scale. Replace all real organizations, people, jurisdictions,
  URLs, dates, distinctive prose, vendor branding, reference identifiers, and
  source assets. Fixtures must not disclose or copy facts from their live
  structural examples.
- Paired cases using one capture with different briefs are especially useful for
  testing scope independently of page understanding.
- Add messy and less literal cases over time, but retain simple policy regression
  cases for important repair and miss behavior.
- Any ground-truth change should be justified by source evidence or an explicit
  product decision, not by model performance during prompt optimization.
