import math
import pickle
import structlog
from normality import normalize, WS
from followthemoney.types import registry
from opensanctions import settings
from opensanctions.core import Dataset
from opensanctions.core.loader import DBEntityLoader, MemoryEntityLoader

log = structlog.get_logger(__name__)

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
TEXT_TYPES = (registry.name, registry.text, registry.string, registry.address)


def ngrams(text, min, max):
    # This is gloriously inefficiently implemented.
    for offset in range(len(text) - min + 1):
        for ngram in range(min, max + 1):
            chunk = text[offset : offset + ngram]
            if len(chunk) == ngram:
                # print(offset, ngram, text, repr(chunk))
                yield chunk


def tokenize_value(type, value):
    """Perform type-specific token generation for a property value."""
    if type == registry.entity:
        return
    value = type.node_id(value)
    if value is not None:
        type_weight = TYPE_WEIGHTS.get(type, 1.0)
        yield value, type_weight
    if type == registry.date and len(value) > 3:
        yield f"y:{value[:4]}", 0.7
    if type in TEXT_TYPES:
        norm = normalize(value, ascii=True, lowercase=True)
        if norm is None:
            return
        for token in norm.split(WS):
            yield f"w:{token}", 0.5
        if type == registry.name:
            for token in ngrams(norm, 2, 4):
                yield f"g:{token}", 0.5


class IndexEntry(object):
    """A set of entities and a weight associated with a given term in the index."""

    __slots__ = "index", "token", "weight", "entities"

    def __init__(self, index, token, weight=1.0):
        self.index = index
        self.token = token
        self.weight = weight
        self.entities = {}

    def add(self, entity_id):
        """Mark the given entity as relevant to the entry's token."""
        if entity_id not in self.entities:
            self.entities[entity_id] = 0
        self.entities[entity_id] += 1

    def remove(self, entity_id):
        self.entities.pop(entity_id, None)
        if not len(self):
            self.index.inverted.pop(self.token, None)

    def tf(self, entity_id):
        """Weighted term frequency for scoring."""
        terms = self.index.terms.get(entity_id, 0)
        tf = self.weight * self.entities.get(entity_id, 0)
        return tf / max(terms, 1)

    def all_tf(self):
        for entity_id in self.entities.keys():
            yield entity_id, self.tf(entity_id)

    def __repr__(self):
        return "<IndexEntry(%r, %r)>" % (self.token, self.weight)

    def __len__(self):
        return len(self.entities)

    def to_dict(self):
        return dict(token=self.token, weight=self.weight, entities=self.entities)

    @classmethod
    def from_dict(cls, index, data):
        obj = cls(index, data["token"], weight=data["weight"])
        obj.entities = data["entities"]
        return obj


class Index(object):
    """An in-memory search index to match entities against a given dataset."""

    def __init__(self, dataset_name):
        self.dataset = Dataset.get(dataset_name)
        self.loader = DBEntityLoader(self.dataset)
        self.inverted = {}
        self.terms = {}

    @property
    def num_entities(self):
        return len(self.terms)

    def tokenize_entity(self, entity, adjacent=False):
        for prop, value in entity.itervalues():
            for token, weight in tokenize_value(prop.type, value):
                yield token, weight
        if adjacent:
            # Index Address, Identification, Sanction, etc.:
            for prop, adjacent in self.loader.get_adjacent(entity):
                for prop, value in adjacent.itervalues():
                    for token, weight in tokenize_value(prop.type, value):
                        yield token, weight * 0.7

    def index(self, entity, adjacent=True):
        """Index one entity. This is not idempodent, you need to remove the
        entity before re-indexing it."""
        terms = 0
        for token, weight in self.tokenize_entity(entity, adjacent=adjacent):
            if token not in self.inverted:
                self.inverted[token] = IndexEntry(self, token, weight=weight)
            self.inverted[token].add(entity.id)
            terms += 1
        self.terms[entity.id] = terms
        log.debug("Index entity", entity=entity, terms=terms)

    def build(self, adjacent=True):
        """Index all entities in the dataset."""
        # Hacky: load to memory for full re-indexes
        if not isinstance(self.loader, MemoryEntityLoader):
            self.loader = MemoryEntityLoader(self.dataset)
        self.inverted = {}
        self.terms = {}
        for entity in self.loader:
            if not entity.target:
                continue
            self.index(entity, adjacent=adjacent)

    def remove(self, entity):
        """Remove an entity from the index."""
        self.terms.pop(entity.id, None)
        for entry in self.inverted.values():
            entry.remove(entity.id)

    def match(self, entity, limit=30):
        """Find entities similar to the given input entity."""
        matches = {}
        for token, _ in self.tokenize_entity(entity):
            entry = self.inverted.get(token)
            if entry is None or len(entry) == 0:
                continue
            for entity_id, tf in entry.all_tf():
                if entity_id not in matches:
                    matches[entity_id] = 0
                idf = math.log(self.num_entities / len(entry))
                matches[entity_id] += tf * idf
        results = sorted(matches.items(), key=lambda x: x[1], reverse=True)
        log.debug("Match entity", entity=entity, results=len(results))
        for entity_id, score in results[:limit]:
            if entity_id == entity.id:
                continue
            yield self.loader.get_entity(entity_id), score

    def save(self):
        with open(self.get_path(self.dataset), "wb") as fh:
            pickle.dump(self, fh)

    @classmethod
    def load(cls, dataset):
        path = cls.get_path(dataset)
        if path.exists():
            with open(path, "rb") as fh:
                return pickle.load(fh)
        index = Index(dataset.name)
        index.build()
        index.save()
        return index

    @classmethod
    def get_path(self, dataset):
        index_dir = settings.DATA_PATH.joinpath("index")
        index_dir.mkdir(exist_ok=True)
        return index_dir.joinpath(f"{dataset.name}.pkl")

    def __getstate__(self):
        """Prepare an index for pickling."""
        return {
            "dataset": self.dataset.name,
            "inverted": [t.to_dict() for t in self.inverted.values()],
            "terms": self.terms,
        }

    def __setstate__(self, state):
        """Restore a pickled index."""
        self.dataset = Dataset.get(state.get("dataset"))
        self.loader = Loader(self.dataset)
        entries = [IndexEntry.from_dict(self, i) for i in state["inverted"]]
        self.inverted = {e.token: e for e in entries}
        self.terms = state.get("terms")
