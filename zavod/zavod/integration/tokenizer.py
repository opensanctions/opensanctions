from normality import WS
from rigour.ids import StrictFormat
from rigour.text.phonetics import metaphone
from rigour.text.scripts import is_modern_alphabet
from typing import Generator, Set, Tuple
from followthemoney.types import registry

from nomenklatura.entity import CE
from nomenklatura.util import fingerprint_name
from nomenklatura.util import name_words, clean_text_basic

WORD_FIELD = "wd"
NAME_PART_FIELD = "np"
PHONETIC_FIELD = "ph"
SKIP = (
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
EMIT_FULL = (
    registry.name,
    registry.address,
    registry.identifier,
    registry.country,
    registry.phone,
    registry.iban,
)
TEXT_TYPES = (
    registry.text,
    registry.string,
    registry.address,
    registry.identifier,
    registry.name,
)


def tokenize_entity(entity: CE) -> Generator[Tuple[str, str], None, None]:
    unique: Set[Tuple[str, str]] = set()
    for prop, value in entity.itervalues():
        type = prop.type
        if not prop.matchable or type in SKIP:
            continue
        if type in EMIT_FULL:
            unique.add((type.name, value[:300].lower()))
        if type in TEXT_TYPES:
            for word in name_words(clean_text_basic(value), min_length=3):
                yield WORD_FIELD, word
        if type == registry.date:
            if len(value) > 4:
                unique.add((type.name, value[:4]))
            unique.add((type.name, value[:10]))
            continue
        if type == registry.name:
            norm = fingerprint_name(value)
            if norm is not None:
                alpha = is_modern_alphabet(value)
                unique.add((type.name, norm))
                for token in norm.split(WS):
                    if len(token) > 2 and len(token) < 30:
                        unique.add((NAME_PART_FIELD, token))
                        yield WORD_FIELD, token
                    if alpha and len(token) > 4:
                        phoneme = metaphone(token)
                        if len(phoneme) > 3:
                            unique.add((PHONETIC_FIELD, phoneme))
            continue
        if type == registry.identifier:
            clean_id = StrictFormat.normalize(value)
            if clean_id is not None:
                unique.add((type.name, clean_id))
            continue

    yield from unique
