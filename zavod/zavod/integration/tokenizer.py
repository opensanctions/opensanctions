import re
from normality import WS
from rigour.ids import StrictFormat
from rigour.addresses import normalize_address
from rigour.names import Name
from rigour.names import tokenize_name, is_stopword, prenormalize_name
from rigour.names import remove_person_prefixes
from rigour.names import tag_person_name, tag_org_name
from typing import Generator, Set, Tuple
from followthemoney import registry, Schema, StatementEntity

NON_LETTER = re.compile(r"[^a-z0-9]+")
WORD_FIELD = "wd"
NAME_PART_FIELD = "np"
PHONETIC_FIELD = "ph"
SYMBOL_FIELD = "sy"
SKIP = (
    # registry.country,
    registry.url,
    registry.topic,
    registry.entity,
    registry.number,
    registry.json,
    registry.gender,
    registry.mimetype,
    registry.ip,
    registry.html,
    registry.checksum,
    registry.language,
)
SKIP_PROPERTIES = {
    "wikidataId",
    "wikipediaUrl",
    "publisher",
    "publisherUrl",
    "programId",
    "recordId",
    "legalForm",
    "status",
}
PREFIXES = {
    registry.name: "n",
    registry.identifier: "i",
    registry.country: "c",
    registry.phone: "p",
    registry.address: "a",
    registry.date: "d",
}
EMIT_FULL = (
    registry.country,
    registry.phone,
)
TEXT_TYPES = (
    registry.text,
    registry.string,
    # registry.address,  # normalized, then added to text type
    registry.identifier,
)


def tokenize_name_(schema: Schema, name: str) -> Generator[Tuple[str, str], None, None]:
    name = name.casefold()
    if schema.is_a("Person"):
        name = remove_person_prefixes(name)
    # Disabled because this has an outsized cost in terms of performance:
    # if schema.is_a("Organization"):
    #     name = remove_org_types(name, normalizer=normalize_name)
    nameobj = Name(name)
    if schema.is_a("Person"):
        nameobj = tag_person_name(nameobj, prenormalize_name)
    elif schema.is_a("LegalEntity"):
        nameobj = tag_org_name(nameobj, prenormalize_name)

    # symbolic_parts: Set[NamePart] = set()
    for span in nameobj.spans:
        val = f"{SYMBOL_FIELD}:{span.symbol.category.value}:{span.symbol.id}"
        yield (SYMBOL_FIELD, val)

        # if len(span.parts) == 1 and span.symbol.category in (
        #     Symbol.Category.NAME,
        #     Symbol.Category.SYMBOL,
        # ):
        #     symbolic_parts.update(span.parts)

    name_tokens: Set[str] = set()
    for part in nameobj.parts:
        name_tokens.add(part.comparable)
        if len(part.form) < 3 or len(part.form) > 30 or is_stopword(part.form):
            continue

        # if part in symbolic_parts:
        #     continue

        yield NAME_PART_FIELD, f"{NAME_PART_FIELD}:{part.comparable}"
        phoneme = part.metaphone
        if phoneme is not None and len(phoneme) > 3:
            yield PHONETIC_FIELD, f"{PHONETIC_FIELD}:{phoneme}"

    name_fp = "".join(sorted(name_tokens))
    if len(name_fp) > 3 and len(name_fp) < 200:
        prefix = PREFIXES.get(registry.name, "n")
        yield (registry.name.name, f"{prefix}:{name_fp}")


def tokenize_entity(entity: StatementEntity) -> Generator[Tuple[str, str], None, None]:
    unique: Set[Tuple[str, str]] = set()
    for prop, value in entity.itervalues():
        type = prop.type
        if not prop.matchable or type in SKIP or prop.name in SKIP_PROPERTIES:
            continue
        prefix = PREFIXES.get(type, type.name)
        if type in EMIT_FULL:
            full_value = value[:300].lower()
            unique.add((type.name, f"{prefix}:{full_value}"))
            continue
        if type in TEXT_TYPES:
            lvalue = value.lower()
            # min 6 to focus on things that could be fairly unique identifiers
            for token in tokenize_name(lvalue, token_min_length=6):
                if is_stopword(token):
                    continue
                yield WORD_FIELD, f"{WORD_FIELD}:{token}"
        if type == registry.date:
            # if len(value) > 4:
            #     unique.add((type.name, value[:4]))
            unique.add((type.name, f"{prefix}:{value[:10]}"))
            continue
        if type == registry.name:
            unique.update(tokenize_name_(entity.schema, value))
            continue
        if type == registry.identifier:
            clean_id = StrictFormat.normalize(value)
            if clean_id is not None:
                unique.add((type.name, f"{prefix}:{clean_id}"))
            continue
        if type == registry.address:
            norm = normalize_address(value)
            if norm is not None:
                # Disable this for now, as it is not performant:
                # norm = remove_address_keywords(norm) or norm
                for word in norm.split(WS):
                    if is_stopword(word):
                        continue
                    if len(word) > 3:
                        unique.add((type.name, f"{prefix}:{word}"))
                    if len(word) > 6:
                        unique.add((WORD_FIELD, f"{WORD_FIELD}:{word}"))

    yield from unique
