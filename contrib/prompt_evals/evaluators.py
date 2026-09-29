import re
from dataclasses import dataclass
from typing import Any

from normality import slugify
from pydantic_evals.evaluators import EvaluationReason, Evaluator, EvaluatorContext
from pydantic_evals.evaluators.evaluator import EvaluatorOutput

from contrib.prompt_evals.models import CaseInputs, CaseMeta, Extraction

COMPARED_FIELDS = [
    "entity_schema",
    "nationality",
    "country",
    "related_url",
    "imo",
    "flag",
]
PARENTHETICAL_RE = re.compile(r"\([^)]*\)|[\"“”'‘’][^\"“”'‘’]*[\"“”'‘’]")


def match_key(name: str) -> str:
    """Normalise a name for matching extracted items to expected items.

    Parenthesised acronyms and quoted nicknames are removed so that a change of
    policy on whether they belong in the name shows up as a name mismatch on a
    matched item, rather than as a missing and a spurious item.
    """
    stripped = PARENTHETICAL_RE.sub(" ", name)
    key = slugify(stripped, sep=" ") or slugify(name, sep=" ") or name.lower()
    # "The Taliban" and "Taliban" are the same entity.
    return re.sub(r"^the ", "", key)


def norm_value(value: Any) -> Any:
    """Normalise a field value for comparison: null and [] are the same, list order
    and string case do not matter."""
    if value is None:
        return []
    if isinstance(value, list):
        return sorted(str(v).strip().casefold() for v in value)
    if isinstance(value, str):
        return value.strip().casefold()
    return value


def pair_items(
    expected: list[dict[str, Any]], output: list[dict[str, Any]], key: str
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], list[str], list[str]]:
    """Pair expected and output items by normalised name, falling back to a
    token-subset match. Returns (pairs, missing expected names, spurious output names)."""
    remaining = {i: match_key(str(item[key])) for i, item in enumerate(output)}
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    missing: list[str] = []
    for exp in expected:
        exp_key = match_key(str(exp[key]))
        found = next((i for i, k in remaining.items() if k == exp_key), None)
        if found is None:
            exp_tokens = set(exp_key.split())
            for i, k in remaining.items():
                tokens = set(k.split())
                shared = exp_tokens & tokens
                if len(shared) >= 2 and (shared == exp_tokens or shared == tokens):
                    found = i
                    break
        if found is None:
            missing.append(str(exp[key]))
        else:
            pairs.append((exp, output[found]))
            remaining.pop(found)
    spurious = [str(output[i][key]) for i in remaining]
    return pairs, missing, spurious


@dataclass
class ItemListMatch(Evaluator[CaseInputs, Extraction, CaseMeta]):
    """Compare a list of extracted items (e.g. designees) with the accepted list.

    Produces: `recall` and `precision` of items by name, `name_exact` for the share
    of matched items whose name is exactly the accepted one, one score per field
    for the share of matched items where the field value equals the accepted value,
    and an `all_match` assertion that passes only when everything agrees.
    Every field the output carries is scored; a field missing from the accepted
    item means it has no value there. Fields present only in the accepted data
    (deprecated fields) are ignored.
    """

    items: str = "designees"
    key: str = "name"

    def evaluate(
        self, ctx: EvaluatorContext[CaseInputs, Extraction, CaseMeta]
    ) -> EvaluatorOutput:
        if ctx.expected_output is None:
            return {}
        expected = list(ctx.expected_output.get(self.items) or [])
        output = list(ctx.output.get(self.items) or [])
        pairs, missing, spurious = pair_items(expected, output, self.key)

        results: dict[str, Any] = {}
        recall = len(pairs) / len(expected) if expected else 1.0
        precision = len(pairs) / len(output) if output else 1.0
        results["recall"] = EvaluationReason(
            value=recall, reason=("missing: " + "; ".join(missing)) if missing else None
        )
        results["precision"] = EvaluationReason(
            value=precision,
            reason=("spurious: " + "; ".join(spurious)) if spurious else None,
        )

        all_match = not missing and not spurious
        if pairs:
            exact = sum(
                1
                for exp, out in pairs
                if str(exp[self.key]).strip() == str(out[self.key]).strip()
            )
            results["name_exact"] = exact / len(pairs)
            all_match = all_match and exact == len(pairs)

            fields = sorted({f for _, out in pairs for f in out if f != self.key})
            for field in fields:
                mismatches = [
                    f"{exp[self.key]}: {exp.get(field)!r} != {out.get(field)!r}"
                    for exp, out in pairs
                    if norm_value(exp.get(field)) != norm_value(out.get(field))
                ]
                score = 1 - len(mismatches) / len(pairs)
                results[field] = EvaluationReason(
                    value=score, reason="; ".join(mismatches) if mismatches else None
                )
                all_match = all_match and not mismatches
        results["all_match"] = all_match
        return results


def describe_edits(
    expected: list[dict[str, Any]], original: list[dict[str, Any]], key: str = "name"
) -> list[str]:
    """Human-readable list of what the reviewer changed between the model's
    original extraction and the accepted extraction."""
    pairs, missing, spurious = pair_items(expected, original, key)
    edits = [f"added: {name}" for name in missing]
    edits += [f"removed: {name}" for name in spurious]
    for exp, orig in pairs:
        if exp[key] != orig[key]:
            edits.append(f"renamed: {orig[key]!r} -> {exp[key]!r}")
        for field in COMPARED_FIELDS:
            if norm_value(exp.get(field)) != norm_value(orig.get(field)):
                edits.append(
                    f"{exp[key]} {field}: {orig.get(field)!r} -> {exp.get(field)!r}"
                )
    return edits
