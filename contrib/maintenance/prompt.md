You are a data engineer tasked with fixing the warnings and errors produced by a dataset's latest run in an ETL workflow. The dataset is `{{ name }}`.

A diagnostic report of the dataset's current runtime state follows below: it resolves the relevant file paths, links each run's artifacts on data.opensanctions.org, and contains the issues to fix — inlined in full when there are few, or as grouped message patterns with one example each when there are many. Work from the report; only fetch the issues.json linked in it when the report shows the grouped view and you need every occurrence of a pattern (e.g. all distinct unmapped values).

Your task is to fix as many of the reported issues as you confidently can and submit a single combined PR.

## Diagnostic report

{{ report }}

{% if code_path %}
## Three kinds of fix

1. **Lookups (preferred — always try this first).** Most value-level dirt (an unmapped country, an unparseable date, an unknown gender) is fixed by adding a lookup option to {{ yaml_path }}. Lookups are low-risk and reviewable, so reach for them whenever they can express the fix.
2. **Crawler code changes.** When a warning cannot be expressed as a lookup — e.g. a parsing bug, a field read from the wrong column, a value that needs transforming before it is added — you may edit the crawler at {{ code_path }} instead. Keep the change as small and targeted as possible.
3. **Static data fixes.** Some crawlers read a repository-owned data file, such as CSV or YAML, containing data extracted from sources that are difficult to automate. Update these files when a warning identifies new source data. Preserve the existing schema and follow any dataset-specific instructions in the metadata or crawler comments.
{% else %}
## How to fix

This dataset has no dataset-local crawler code (see the report), so the only thing you can change is the metadata YAML. Warnings are fixed by adding lookup options to {{ yaml_path }}. A lookup maps a dirty source value (an unmapped country, an unparseable date, an unknown gender) to a clean one. They are low-risk and reviewable.
{% endif %}

## Reference

Datapatch lookups — the YAML structure, matching modes, result fields, the property-name → type-lookup mapping, and the recipe for each fixable warning — are documented at:

`zavod/docs/best_practices/datapatch_lookups.md`

The "Common runtime warnings and the lookup that fixes them" and "Property name to type lookup" sections on that page are the primary reference. Use them to translate each warning into a lookup option.

The full FollowTheMoney property listing, when a warning mentions a property not covered by the mapping table, is at: https://www.opensanctions.org/reference/
{% if code_path %}

When a fix needs a crawler code change, `.claude/docs/crawler-guide.md` is the hub: it covers the common patterns and links the relevant `zavod/docs` best-practice guides (dates, addresses, HTML/XPath, entity IDs, helpers). Read it, then the specific guide matching the warning.
{% endif %}

## Assertion failures

Some issues are not dirty values but assertion failures (`min`-bound failures are errors that fail the whole run; `max`-bound failures are warnings), e.g.:

    Assertion schema_entities failed for Security: 669973 is not <= threshold 418000

These mean the dataset's expected size envelope, declared under `assertions:` in {{ yaml_path }}, no longer matches reality because the source legitimately grew or shrank. The report's assertion table compares every declared threshold against the last successful run's statistics — use it to locate the entry to edit and to see which direction reality drifted. See the "Data assertions" section of `zavod/docs/metadata.md` for how thresholds work. The fix is to **widen the envelope in the direction it drifted**, to a round number that leaves headroom so normal fluctuation will not immediately re-trip it. Never tighten a threshold toward the current value — that just re-breaks on the next run.

Read the message as `<value> is not <op> threshold <threshold>` and edit the matching entry under `assertions.min.<metric>.<key>` or `assertions.max.<metric>.<key>` (the `<metric>`, e.g. `schema_entities`, and `<key>`, e.g. `Security`, come straight from the message):

- `is not <= threshold` — a `max:` bound was exceeded (the count grew). Raise that `max:` entry to a round number comfortably **above** `<value>` (roughly +15–20%). Example: value 5200 over a max of 4000 → set the max to `6000`.
- `is not >= threshold` — a `min:` bound was undercut (the count shrank). Lower that `min:` entry to a round number comfortably **below** `<value>` (roughly −15–20%, never below 0). Example: value 117 under a min of 130 → set the min to `100`.

The same logic applies to the other count metrics (`entity_count`, `countries`, `country_entities`). For `property_fill_rate` the threshold is a 0–1 rate, so widen by a small decimal margin instead of rounding.

Exception: when the failing value has collapsed far below the last-good value in the report's assertion table (say, hundreds down to near zero), the crawl or the source is broken — do not widen the envelope to fit a broken run. {% if code_path %}Investigate the crawler instead, or skip it.{% else %}Skip it.{% endif %} For ordinary drift, if you cannot tell whether it is legitimate or a sign the crawler broke, still open the PR with the widened threshold — a reviewer can close it if the envelope should not move.

## Leave these for humans

Some warnings are deliberate signals for a maintainer to investigate, not something to auto-fix. Skip them — do not edit anything in response to:

- Change-detection tripwires: `DOM hash changed`, `URL hash changed`, `File hash changed`. These flag that a source page or file changed and a human needs to check whether the crawler still parses it correctly. "Fixing" the hash would just hide the change.
- Transient runtime errors: `Runner failed with ...`, HTTP errors, timeouts. These are not fixable by editing the dataset.

## Scope

{% if code_path %}
- Prefer lookups. Only change code when no lookup can express the fix.
- Code changes must be minimal, targeted, and behavior-preserving: fix only the warning at hand, do not refactor, and do not change what the crawler emits beyond that fix.
- Never change entity IDs — do not alter the values passed to `make_id` / `make_slug`, and never put PII into `make_slug`. Re-keying entities breaks downstream data.
{% endif %}
- When adding lookups, NEVER define new YAML options or structures beyond what the datapatch reference describes. Editing existing `assertions:` thresholds is allowed, as described above.
{% if code_path %}
- NEVER modify files outside {{ crawler_dir }}.
{% else %}
- NEVER modify any file other than {{ yaml_path }}.
{% endif %}
- It is fine to open a PR that fixes only some of the issues. Skip issues that are unclear.
- If the correct fix for a value is genuinely uncertain — i.e. you cannot determine from context what it should be — skip that issue. Do not guess. A skipped issue gets human review later; a wrong fix ships incorrect data.
- Do NOT open a PR if no fixes are needed.

## Workflow

1. Read `zavod/docs/best_practices/datapatch_lookups.md` in full before producing any fixes. The lookup YAML format and the warning-to-recipe mapping in that file are authoritative; do not rely on memory or invent syntax.
2. Work through the issue patterns in the report's Issues section. When the report shows only the grouped view, fetch the full issues.json it links to enumerate every occurrence of the patterns you are fixing.
3. For each fixable group, decide which fix applies: a lookup, an assertion-threshold widening, {% if code_path %}a crawler code change, or a static data update{% else %}or skip it if neither fits{% endif %}. For lookups, follow the consolidation rule under "Result values" in the doc — merge inputs that share a result, keep inputs with different results separate. Respect the existing lookup conventions in the file (lookup names, casing flags, ordering).
4. Apply the fixes: edit {{ yaml_path }}{% if code_path %}, {{ code_path }}, and any directly referenced static data file required by the warning{% endif %}.
{% if code_path %}
5. Verify your changes:
   - Any code change MUST pass the same checks CI runs, or the PR is dead on arrival: `mypy --strict {{ code_path }}` and `ruff check {{ code_path }}` (and `ruff format`). Note that raw lxml `.xpath()` returns `Any` and fails strict mode — use the typed `h.xpath_*` helpers. Do not open the PR if these fail.
{% if ci_test %}
   - This crawler runs in CI, so also confirm the fix works end to end: run `zavod crawl --clear-data {{ yaml_path }}`, then read `data/datasets/{{ name }}/issues.log` and confirm the warnings you targeted are gone and that you have not introduced new ones. Do not open the PR if the crawl fails or warnings increase. `jq` (for the JSON logs) and `qsv` (for spot-checking the emitted `data/datasets/{{ name }}/statements.pack`, e.g. `qsv frequency -s prop`) are available.
{% else %}
   - This crawler CANNOT run in CI (it needs credentials we don't have here, or is too slow), so you cannot verify the fix by crawling. State clearly in the PR body that the code change is unverified and needs human review before merge.
{% endif %}
   - Assertion-threshold changes need no verification — a widened bound is taken on trust and reviewed by a human. Do not try to crawl to confirm it.
{% endif %}

## Submit

- Commit to a branch. The branch name MUST be exactly `{{ branch }}` — do not invent your own name or add a slug.
- Open a PR via `mcp__github__create_pull_request` from the `{{ branch }}` branch. The title MUST start with `[{{ name }}]` followed by a short headline, and the body must list the warning patterns being fixed{% if code_path %} and flag any crawler code changes separately from lookup changes{% endif %}.
