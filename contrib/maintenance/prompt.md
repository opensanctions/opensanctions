Fix the warnings and errors from the latest run of the `{{ name }}` dataset and submit one combined PR containing the fixes you can support with evidence.

The diagnostic report below is the source of runtime facts. It resolves the dataset files, links the run artifacts and shows the issues in full or grouped by message pattern. When it shows grouped issues, fetch the linked issues.json only if you need to enumerate every value in a pattern.

## Diagnostic report

{{ report }}

## Choose the correct kind of fix

- **Datapatch lookup — preferred for source-specific exceptions.** Use a lookup when known source values or classes of values can be corrected with exact, `contains` or regex matching. Prefer this to accumulating literal branches and regular expressions in crawler code. Before editing a lookup, read `zavod/docs/best_practices/datapatch_lookups.md` and follow its warning recipes, result-consolidation rules and existing conventions in {{ yaml_path }}.
- **Assertion bound.** Unless the report shows collapsed or explosive output or evidence of a crawler or source-access failure, propose a bounded assertion update for human review even when you cannot determine whether the drift is legitimate. State that uncertainty and the available evidence in the PR. For count metrics, lower a failing minimum to a sensible round value near 80% of the observed count, or raise a failing maximum to roughly twice the observed count. For fill rates, use a small margin within 0–1. The shared rules are in `zavod/docs/metadata.md#maintaining-assertion-bounds`.
- **Metadata correction.** Correct a source URL or other dataset configuration when the report and current source show that the metadata is wrong.
{% if code_path %}
- **Crawler code.** Change {{ code_path }} when the problem is systematic parsing or transformation logic that should work for unseen values, rather than an enumerable set of source exceptions. Search `zavod/docs` for the specific helper or best-practice guide relevant to the change.
- **Static data.** Update repository-owned CSV, YAML or other static data in {{ crawler_dir }} when it contains a maintained extraction of source information.
{% endif %}
## Change-detection warnings

For `DOM hash changed`, `URL hash changed` and `File hash changed`, follow `zavod/docs/best_practices/change_detection.md`. Inspect and understand the changed source; inference-based extraction from an unstructured page or document is allowed.

For PDFs, start with `pdftotext -layout <file> -`; use `pdftoppm -png <file> <prefix>` when visual structure or scanned content makes text extraction ambiguous.

- If source data changed, incorporate it into the dataset before accepting the new hash.
- If the change is demonstrably cosmetic, the new hash may be accepted, but the PR must explain what changed and why dataset output is unaffected. Improve the monitor scope when the same noise is likely to recur.
- If the impact is unresolved, retain the old hash and skip the issue.

`There are N unaccepted items for dataset ...` is review-system backlog, cleared outside this repository. Do not make a repository change for that warning alone.

`jq` is available for inspecting JSON issue logs, and `qsv` for checking `statements.pack`, for example `qsv frequency -s prop <file>`.

## Execution boundary

{% if code_path %}
You may modify {{ yaml_path }}, {{ code_path }} and directly related static data inside {{ crawler_dir }}. Do not modify files outside {{ crawler_dir }}.

- Keep crawler changes minimal and limit output differences to those justified by the reported issues.
- Preserve entity IDs: do not change inputs to `make_id` or `make_slug`, and never put PII into `make_slug`.
{% else %}
No dataset-local crawler code is available. Modify only {{ yaml_path }} and skip issues that require crawler or shared-framework changes.
{% endif %}

Investigate every reported pattern, but change only what you can resolve confidently from the report, source data and documentation, except for the explicitly reviewable assertion-bound proposals above. A partial fix is valid. Do not guess, and do not open a PR when nothing warrants a repository change.

## Verify

Run the exact static checks used by dataset CI:

    contrib/lint_dataset.sh {{ yaml_path }}

It may apply formatting fixes. Resolve every finding and rerun it until it prints `lint_dataset: OK`.
{% if ci_test %}
Run a clean crawl when the change is significant enough that end-to-end behavior needs verification; routine lookup and assertion fixes generally need only lint.

    zavod crawl --clear-data {{ yaml_path }}

When you crawl, read `data/datasets/{{ name }}/issues.log`. The issues you targeted must be gone and no new warnings or errors may have appeared. Do not open the PR if the crawl fails or makes the issue set worse.
{% else %}
This dataset cannot be crawled in CI; lint is the available verification. Do not attempt a crawl.
{% endif %}

## Submit

After investigating all patterns and verifying all intended changes:

1. Commit and push the changes on the branch named exactly `{{ branch }}`.
2. Open one PR using `mcp__github__create_pull_request` from that branch. The title must start with `[{{ name }}]` followed by a short headline.
3. In the body, list the warning patterns addressed, the evidence for each fix, the applicable lookup, assertion, metadata, crawler or static-data changes, and the verification performed. Explain why an accepted cosmetic hash change has no data impact, and flag crawler or static-data changes that could not be crawl-verified.

If no repository change is justified, do not commit or open a PR.
