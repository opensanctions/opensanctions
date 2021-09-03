import math
from followthemoney import model
from normality import normalize, WS
from followthemoney.types import registry
from opensanctions import settings
from opensanctions.core import Dataset
from opensanctions.exporters.index import ExportIndex

TYPE_WEIGHTS = {
    registry.country: 0.2,
    registry.date: 0.2,
    registry.language: 0.2,
    registry.iban: 2.0,
    registry.phone: 2.0,
    registry.email: 2.0,
    registry.entity: 0.0,
    registry.topic: 0.1,
    registry.address: 1.2,
    registry.identifier: 1.5,
}


def typed_token(type_, value):
    return f"{type_}:{value}"


# def word_weight(word):
#     return 0.2 * (math.sqrt(len(word)) - 1)


def ngrams(text, min, max):
    # This is gloriously inefficiently implemented.
    for offset in range(len(text) - min + 1):
        for ngram in range(min, max + 1):
            chunk = text[offset : offset + ngram]
            if len(chunk) == ngram:
                # print(offset, ngram, text, repr(chunk))
                yield chunk


def tokenize_value(type, value):
    if type in (registry.name, registry.text, registry.string, registry.address):
        norm = normalize(value, ascii=True)
        if norm is not None:
            for token in norm.split(WS):
                yield typed_token("word", token), 0.5
        if type == registry.name:
            for token in ngrams(norm, 2, 4):
                yield typed_token("ng", token), 0.5
    value = type.node_id(value)
    if value is not None:
        type_weight = TYPE_WEIGHTS.get(type, 1.0)
        yield value, type_weight


class IndexEntry(object):
    __slots__ = "index", "token", "weight", "entities"

    def __init__(self, index, token, weight=1.0):
        self.index = index
        self.token = token
        self.weight = weight
        self.entities = {}

    @property
    def num_entities(self):
        return len(self.entities)

    def add(self, entity_id):
        if entity_id not in self.entities:
            self.entities[entity_id] = 0
        self.entities[entity_id] += 1

    def remove(self, entity_id):
        self.entities.pop(entity_id, None)

    def tf(self, entity_id):
        terms = self.index.terms.get(entity_id, 0)
        tf = self.weight * self.entities.get(entity_id, 0)
        return tf / max(terms, 1)

    def all_tf(self):
        for entity_id in self.entities.keys():
            yield entity_id, self.tf(entity_id)

    def __repr__(self):
        return "<IndexEntry(%r, %r)>" % (self.token, self.weight)


class Index(object):
    def __init__(self, dataset_name):
        self.dataset = Dataset.get(dataset_name)
        self.cache = ExportIndex(self.dataset)
        self.inverted = {}
        self.terms = {}

    @property
    def num_entities(self):
        return len(self.terms)

    def tokenize_entity(self, entity):
        for prop, value in entity.itervalues():
            for token, weight in tokenize_value(prop.type, value):
                yield token, weight

    def index_entity(self, entity):
        terms = 0
        for token, weight in self.tokenize_entity(entity):
            if token not in self.inverted:
                self.inverted[token] = IndexEntry(self, token, weight=weight)
            self.inverted[token].add(entity.id)
            terms += 1
        self.terms[entity.id] = terms

    def build(self):
        self.inverted = {}
        self.terms = {}
        for entity in self.cache.entities.values():
            if not entity.target:
                continue
            self.index_entity(entity)
        print("BUILT INDEX", len(self.inverted), len(self.terms))

    def remove(self, entity):
        self.terms.pop(entity.id, None)
        for entry in self.inverted.values():
            entry.remove(entity.id)

    def match(self, entity, limit=30):
        matches = {}
        for token, _ in self.tokenize_entity(entity):
            entry = self.inverted.get(token)
            if entry is None or entry.num_entities == 0:
                continue
            for entity_id, tf in entry.all_tf():
                if entity_id not in matches:
                    matches[entity_id] = 0
                idf = math.log(self.num_entities / entry.num_entities)
                matches[entity_id] += tf * idf
        results = sorted(matches.items(), key=lambda x: x[1], reverse=True)
        for entity_id, count in results[:limit]:
            # score = count / self.terms[entity_id]
            score = count
            print(self.cache.get_entity(entity_id), score)


if __name__ == "__main__":
    index = Index("default")
    index.build()

    entity = model.make_entity("Person")
    entity.add("name", "Keven")
    index.match(entity)
