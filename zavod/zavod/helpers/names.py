from collections import defaultdict
from dataclasses import dataclass
import re
from typing import Dict, List, Optional, Tuple, cast

from followthemoney.util import join_text
from normality import squash_spaces
from pydantic import JsonValue
from rigour.names import contains_split_phrase
from rigour.text import is_nullword

from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.extract.names.clean import (
    LLM_MODEL_VERSION,
    SourceNames,
    Names,
)

# alias clean_names so that it could be imported from here
from zavod.extract.names.clean import clean_names as clean_names
from zavod.stateful.review import (
    JSONSourceValue,
    Review,
    review_extraction,
)

REGEX_AND = re.compile(r"(\band\b|&|\+)", re.I)
REGEX_LNAME_FNAME = re.compile(r"^\w+, \w+$", re.I)
REGEX_CLEAN_COMMA = re.compile(
    r", \b(LLC|L\.L\.C|Inc|Jr|INC|L\.P|LP|Sr|III|II|IV|S\.A|LTD|USA INC|\(?A/K/A|\(?N\.K\.A|\(?N/K/A|\(?F\.K\.A|formerly known as|INCORPORATED)\b",  # noqa
    re.I,
)


def make_name(
    full: Optional[str] = None,
    name1: Optional[str] = None,
    first_name: Optional[str] = None,
    given_name: Optional[str] = None,
    name2: Optional[str] = None,
    second_name: Optional[str] = None,
    middle_name: Optional[str] = None,
    name3: Optional[str] = None,
    patronymic: Optional[str] = None,
    matronymic: Optional[str] = None,
    name4: Optional[str] = None,
    name5: Optional[str] = None,
    tail_name: Optional[str] = None,
    last_name: Optional[str] = None,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
) -> Optional[str]:
    """Provides a standardised way of assembling the components of a human name.
    This does a whole lot of cultural ignorance work, so YMMV.

    Args:
        full: The full name if available (this will otherwise be generated).
        name1: The first name if numeric parts are used.
        first_name: The first name.
        given_name: The given name (also first name).
        name2: The second name if numeric parts are used.
        second_name: The second name.
        middle_name: The middle name.
        name3: The third name if numeric parts are used.
        patronymic: The patronymic (father-derived) name.
        matronymic: The matronymic (mother-derived) name.
        name4: The fourth name if numeric parts are used.
        name5: The fifth name if numeric parts are used.
        tail_name: A secondary last name.
        last_name: The last/family name name.
        prefix: A prefix to the name (e.g. "Mr").
        suffix: A suffix to the name (e.g. "Jr").

    Returns:
        The full name.
    """
    if full is not None:
        full = squash_spaces(full)
        if len(full) > 0:
            return full
    return join_text(
        prefix,
        name1,
        first_name,
        given_name,
        name2,
        second_name,
        middle_name,
        name3,
        patronymic,
        matronymic,
        name4,
        name5,
        tail_name,
        last_name,
        suffix,
    )


def set_name_part(
    entity: Entity,
    prop: str,
    value: Optional[str],
    quiet: bool,
    lang: Optional[str],
    origin: Optional[str],
) -> None:
    if value is None:
        return
    prop_ = entity.schema.get(prop)
    if prop_ is None:
        if quiet:
            return
        raise TypeError("Invalid prop: %s [value: %r]" % (prop, value))
    entity.add(prop_, value, lang=lang, origin=origin)


def apply_name(
    entity: Entity,
    full: Optional[str] = None,
    name1: Optional[str] = None,
    first_name: Optional[str] = None,
    given_name: Optional[str] = None,
    name2: Optional[str] = None,
    second_name: Optional[str] = None,
    middle_name: Optional[str] = None,
    name3: Optional[str] = None,
    patronymic: Optional[str] = None,
    matronymic: Optional[str] = None,
    name4: Optional[str] = None,
    name5: Optional[str] = None,
    tail_name: Optional[str] = None,
    last_name: Optional[str] = None,
    maiden_name: Optional[str] = None,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    alias: bool = False,
    name_prop: str = "name",
    is_weak: bool = False,
    quiet: bool = False,
    lang: Optional[str] = None,
    origin: Optional[str] = None,
) -> None:
    """A standardised way to set a name for a person or other entity, which handles
    normalising the categories of names found in source data to the correct properties
    (e.g. "family name" becomes "lastName").

    Args:
        entity: The entity to set the name on.
        full: The full name if available (this will otherwise be generated).
        name1: The first name if numeric parts are used.
        first_name: The first name.
        given_name: The given name (also first name).
        name2: The second name if numeric parts are used.
        second_name: The second name.
        middle_name: The middle name.
        name3: The third name if numeric parts are used.
        patronymic: The patronymic (father-derived) name.
        matronymic: The matronymic (mother-derived) name.
        name4: The fourth name if numeric parts are used.
        name5: The fifth name if numeric parts are used.
        tail_name: A secondary last name.
        last_name: The last/family name name.
        maiden_name: The maiden name (before marriage).
        prefix: A prefix to the name (e.g. "Mr").
        suffix: A suffix to the name (e.g. "Jr").
        alias: If this is an alias name.
        name_prop: The property to set the full name on.
        is_weak: If this is a weak alias name.
        quiet: If this should not raise errors on invalid properties.
        lang: The language of the name.
        origin: The origin of the name (e.g. a GPT model).
    """
    if not is_weak:
        set_name_part(entity, "firstName", given_name, quiet, lang, origin)
        set_name_part(entity, "firstName", first_name, quiet, lang, origin)
        set_name_part(entity, "secondName", second_name, quiet, lang, origin)
        set_name_part(entity, "middleName", middle_name, quiet, lang, origin)
        set_name_part(entity, "fatherName", patronymic, quiet, lang, origin)
        set_name_part(entity, "motherName", matronymic, quiet, lang, origin)
        set_name_part(entity, "lastName", last_name, quiet, lang, origin)
        set_name_part(entity, "lastName", maiden_name, quiet, lang, origin)
        set_name_part(entity, "firstName", name1, quiet, lang, origin)
        set_name_part(entity, "secondName", name2, quiet, lang, origin)
        set_name_part(entity, "middleName", name3, quiet, lang, origin)
        set_name_part(entity, "middleName", name4, quiet, lang, origin)
        set_name_part(entity, "middleName", name5, quiet, lang, origin)
        set_name_part(entity, "lastName", tail_name, quiet, lang, origin)
    if alias:
        name_prop = "alias"
    if is_weak:
        name_prop = "weakAlias"
    full = make_name(
        full=full,
        name1=name1,
        first_name=first_name,
        given_name=given_name,
        name2=name2,
        second_name=second_name,
        middle_name=middle_name,
        name3=name3,
        patronymic=patronymic,
        matronymic=matronymic,
        name4=name4,
        name5=name5,
        tail_name=tail_name,
        last_name=last_name,
        prefix=prefix,
        suffix=suffix,
    )
    if full is not None and len(full):
        entity.add(name_prop, full, quiet=quiet, lang=lang, origin=origin)


def split_comma_names(context: Context, text: str) -> List[str]:
    """Split a string of multiple names that may contain company and individual names,
    some including commas, into individual names without breaking partnership names
    like "A, B and C Inc" or individuals like "Smith, Jane".

    To make life easier, commas are stripped from company type suffixes like "Blue, LLC"

    If the string can't be split into whole names reliably, a datapatch is looked up
    under the `comma_names` key, which should contain a list of names in the `names`
    attribute. If no match is found, the name is returned as a single item list,
    and a warning emitted.
    """
    text = squash_spaces(text)
    if len(text) == 0:
        return []

    text = REGEX_CLEAN_COMMA.sub(r" \1", text)
    # If the string ends in a comma, the last comma is unnecessary (e.g. Goldman Sachs & Co. LLC,)
    if text.endswith(","):
        text = text[:-1]

    if not REGEX_AND.search(text) and not REGEX_LNAME_FNAME.match(text):
        names = [n.strip() for n in text.split(",")]
        return names
    else:
        if ("," in text) or (" and " in text):
            res = context.lookup("comma_names", text)
            if res:
                return cast("List[str]", res.names)
            else:
                context.log.warning("Not sure how to split on comma or and.", text=text)
                return [text]
        else:
            return [text]


@dataclass
class Regularity:
    is_irregular: bool
    suggested_prop: Optional[str] = None


def check_name_regularity(entity: Entity, string: Optional[str]) -> Regularity:
    """Determine whether a name string potentially needs cleaning."""
    string = squash_spaces(string or "")

    if not string:
        return Regularity(is_irregular=False)

    # Do the prop-suggesting checks first so that we hit them before
    # the less specific checks which are less helpful when they don't
    # suggest a prop.

    # This is where we'll move the heuristic checks which can suggest better
    # categorisation of names:
    #
    ## Single token Person name (after stripping prefixes) -> weakAlias
    # if entity.schema.is_a("Person"):
    #    deprefixed = remove_person_prefixes(string)
    #    if " " not in deprefixed:
    #        return Regularity(is_irregular=True, suggested_prop="weakAlias")
    #
    ## Organization name shorter than 8 letters, all uppercase -> abbreviation
    # if entity.schema.is_a("Organization"):
    #    if len(string) < 8 and string.isupper():
    #        return Regularity(is_irregular=True, suggested_prop="abbreviation")

    spec = entity.dataset.names.get_spec(entity.schema)
    if spec:
        for char in spec.reject_chars_consolidated:
            if char in string:
                return Regularity(is_irregular=True)

        # is nullword
        if not spec.allow_nullwords and is_nullword(string, normalize=True):
            return Regularity(is_irregular=True)

        # min length
        if len(string) < spec.min_chars:
            return Regularity(is_irregular=True)

        # single token min length
        if " " not in string and len(string) < spec.single_token_min_length:
            return Regularity(is_irregular=True)

        # requires space
        if spec.require_space and " " not in string:
            return Regularity(is_irregular=True)

    # contains a known-as phrase
    if contains_split_phrase(string):
        return Regularity(is_irregular=True)

    return Regularity(is_irregular=False)


def is_name_irregular(entity: Entity, string: Optional[str]) -> bool:
    """Determine whether a name string is irregular and needs cleaning."""
    return check_name_regularity(entity, string).is_irregular


def check_names_regularity(entity: Entity, names: Names) -> Tuple[bool, Names]:
    """
    Determine whether any name string in the given Names instance is irregular
    and needs cleaning.

    Returns a tuple of a boolean indicating whether any name string is irregular,
    and a Names instance based on that supplied, with any heuristic-suggested
    categorisation adjustments applied (e.g. suggesting that a name be moved
    from "name" to "alias" or "weakAlias").
    """
    is_irregular = False
    updated_suggested_data = defaultdict(list)
    for key, strings in names.nonempty_item_lists():
        for string in strings:
            regularity = check_name_regularity(entity, string)
            if regularity.is_irregular:
                is_irregular = True
            if regularity.suggested_prop is None:
                updated_suggested_data[key].append(string)
            else:
                updated_suggested_data[regularity.suggested_prop].append(string)
    updated_suggested = Names(**updated_suggested_data)
    return is_irregular, updated_suggested


def derive_original_values(
    original: Names, extracted: Names
) -> Dict[str, Optional[str]]:
    """
    Derive an original_value for each value in extracted based on the values in original.

    For each value in extracted:
        If there's exactly one value in original, use that for all names.
        If some value in original matches it exactly, leave blank - no original_value needed.
        If some value in original contains it, we can use that the value from original as original_value.
        Otherwise leave blank - this is best-effort only.
    """
    original_values: List[str] = []
    for _prop, values in original.nonempty_item_lists():
        original_values.extend(values)

    derived_originals: Dict[str, Optional[str]] = {}
    for _prop, extracted_values in extracted.nonempty_item_lists():
        for extracted_value in extracted_values:
            if len(original_values) == 1:
                derived_originals[extracted_value] = original_values[0]
            else:
                for value in original_values:
                    if value == extracted_value:
                        continue
                    elif extracted_value in value:
                        derived_originals[extracted_value] = value
                        break
    return derived_originals


def apply_names(
    entity: Entity,
    *,
    original: Names,
    names: Names,
    lang: Optional[str] = None,
    origin: Optional[str] = None,
) -> None:
    """
    Apply the given names to the entity in the indicated props.

    If original contains more than one value, an original value is derived on
    best-effort a basis from the original names.

    Args:
        entity: The entity to apply names to.
        original: Original names, used if original_value needs to be derived.
        names: The names to apply to the entity, potentially altered or re-categorised from original.
        lang: The language all names, if known.
        origin: The origin of apply_names (e.g. a GPT model name)
    """
    derived_originals = derive_original_values(original, names)

    for prop, name_values in names.nonempty_item_lists():
        for name in name_values:
            entity.add(
                prop,
                name,
                lang=lang,
                origin=origin,
                original_value=derived_originals.get(name),
            )


def review_key_parts(entity: Entity, original: Names) -> List[str]:
    # Only use the non-empty props in the key so that adding props in
    # future doesn't change the key unless they're actually populated.
    key_parts = [entity.schema.name]
    for prop, strings in original.nonempty_item_lists():
        key_parts.append(prop)
        key_parts.extend(strings)
    return key_parts


def _review_names(
    context: Context,
    entity: Entity,
    original: Names,
    suggested: Optional[Names] = None,
    llm_cleaning: bool = False,
) -> Review[Names]:
    """
    Post the given names for review, optionally after LLM-based cleaning, and return the review.

    Assumes that if suggested is supplied, it differs from original.
    """
    source_names = SourceNames(entity_schema=entity.schema.name, original=original)

    if llm_cleaning:
        if settings.OPENAI_API_KEY is None:
            context.log.warning(
                "LLM cleaning enabled but OPENAI_API_KEY not configured, falling back to non-LLM review."
            )
            origin = "analyst"
        else:
            suggested = clean_names(context, source_names)
            origin = LLM_MODEL_VERSION
    else:
        origin = "analyst"

    # We don't include suggested in the key so that we don't automatically invalidate
    # the reviews just by changing heuristic or LLM suggestions.
    key_parts = review_key_parts(entity, original)

    # Only include the populated props in the source value for human readability
    source_value_data: Dict[str, str | Dict[str, List[str]]] = {
        "entity_schema": entity.schema.name
    }
    populated_props = dict(source_names.original.nonempty_item_lists())
    source_value_data["original"] = populated_props

    source_value = JSONSourceValue(
        key_parts=key_parts,
        label="names",
        data=cast(JsonValue, source_value_data),
    )
    original_extraction = suggested or original
    original_extraction = original_extraction.simplify()
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=original_extraction,
        origin=origin,
    )
    review.link_entity(context, entity)
    return review


def review_names(
    context: Context,
    entity: Entity,
    *,
    original: Names,
    suggested: Optional[Names] = None,
    is_irregular: bool = False,
    llm_cleaning: bool = False,
) -> Optional[Review[Names]]:
    """
    Determines whether names need cleaning and if so, posts them for review.

    If 'suggested' is not supplied, 'check_names_regularity' is used to determine
    if cleaning or review is needed, and potentially suggest categorisation.

    Names are considered to have been pre-determined to need cleaning/review if
    'is_irregular' is passed as True, or if 'suggested'
    is supplied and differs from 'original'. Crawlers that do their own suggestions
    should normally do those on the result of check_names_regularity, so that
    its suggestions don't override the crawler's suggestions.

    If 'llm_cleaning' is True, an LLM-based cleaning step is additionally done
    on 'suggested' if provided, otherwise on 'original', before posting for review.
    Any categorisation in 'original' and 'suggested' is disregarded and left to the LLM
    to determine. This can not be used with crawler-supplied suggestions and,
    and heuristic suggestions are not passed to the LLM.

    Returns None if no cleaning/review is needed and the original can be applied as-is.

    Args:
        context: The current context.
        entity: The entity to apply names to.
        original: The original categorisation of names. This is to convey to the
            analyst how the source data categorised the name string(s).
        suggested: The suggested categorisation of names. This contains an initial
            categorisation where the source dataset might have adjusted the categorisation
            based on heuristics specific to that dataset.
        llm_cleaning: Whether to use LLM-based name cleaning.
        apply: Whether to apply the names to the entity.
    """

    if original.is_empty():
        return None

    if llm_cleaning:
        assert suggested is None, (
            "Suggested names can't be supplied if LLM cleaning is enabled"
        )

    # heuristic-based review unless suggestion was supplied
    if suggested is None:
        is_irregular_, suggested = check_names_regularity(entity, original)
        is_irregular = is_irregular or is_irregular_

    # heuristics didn't identify irregularity, and the crawler didn't suggest
    # re-categorisation, there's nothing to review.
    if not is_irregular and suggested == original:
        return None

    # human and optionally LLM-based review
    return _review_names(
        context,
        entity,
        original=original,
        suggested=suggested,
        llm_cleaning=llm_cleaning,
    )


def apply_reviewed_names(
    context: Context,
    entity: Entity,
    *,
    original: Names,
    suggested: Optional[Names] = None,
    is_irregular: bool = False,
    lang: Optional[str] = None,
    llm_cleaning: bool = False,
) -> None:
    """
    Determines whether names need cleaning and if so, posts them for review.

    If 'suggested' is not supplied, 'check_names_regularity' is used to determine
    if cleaning or review is needed, and potentially suggest categorisation.

    Names are considered to have been pre-determined to need cleaning/review if
    'is_irregular' is passed as True, or if 'suggested'
    is supplied and differs from 'original'. Crawlers that do their own suggestions
    should normally do those on the result of check_names_regularity, so that
    its suggestions don't override the crawler's suggestions.

    If 'llm_cleaning' is True, an LLM-based cleaning step is additionally done
    on 'suggested' if provided, otherwise on 'original', before posting for review.
    Any categorisation in 'original' and 'suggested' is disregarded and left to the LLM
    to determine. This can not be used with crawler-supplied suggestions and,
    and heuristic suggestions are not passed to the LLM.

    Args:
        context: The current context.
        entity: The entity to apply names to.
        original: The original string(s) and their categorisation according to the data source.
        suggested: Optional suggestion of different categorisation of names.
        lang: The language of the name, if known.
        llm_cleaning: Whether to use LLM-based name cleaning.
    """
    review = review_names(
        context,
        entity,
        original=original,
        suggested=suggested,
        is_irregular=is_irregular,
        llm_cleaning=llm_cleaning,
    )

    if review is None or not review.accepted:
        apply_names(entity, original=original, names=original, lang=lang)
        return

    apply_names(
        entity,
        original=original,
        names=review.extracted_data,
        lang=lang,
        origin=review.origin,
    )


def apply_reviewed_name_string(
    context: Context,
    entity: Entity,
    *,
    string: Optional[str],
    original_prop: str = "name",
    lang: Optional[str] = None,
    llm_cleaning: bool = False,
) -> None:
    """
    Clean the name(s) in the provided string if needed, then post them for review.

    Cleaned names are applied to an entity if accepted, potentially to a different
    property from 'original_prop' if cleaning proposed an alternative which was accepted
    or modified in review.

    Unaccepted reviews result in the name being applied to 'original_prop'.

    Also falls back to 'original_prop' with a warning if llm_cleaning is True
    but the LLM service is not configured.

    Args:
        context: The current context.
        entity: The entity to apply names to.
        string: The raw name(s) string.
        original_prop: The original property for the name according to the data source.
        lang: The language of the name, if known.
        llm_cleaning: Whether to use LLM-based name cleaning.
    """
    original = Names(**{original_prop: string})

    apply_reviewed_names(
        context,
        entity,
        original=original,
        suggested=None,
        lang=lang,
        llm_cleaning=llm_cleaning,
    )
