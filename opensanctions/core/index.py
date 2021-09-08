import math
import pickle
import structlog
from normality import normalize, WS
from followthemoney.types import registry
from opensanctions import settings
from opensanctions.core.dataset import Dataset
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

    __slots__ = "index", "token", "weight", "entities", "frequencies"

    def __init__(self, index, token, weight=1.0):
        self.index = index
        self.token = token
        self.weight = weight
        self.entities = {}
        self.frequencies = {}

    def add(self, entity_id):
        """Mark the given entity as relevant to the entry's token."""
        if entity_id not in self.entities:
            self.entities[entity_id] = 0
        self.entities[entity_id] += 1

    def remove(self, entity_id):
        self.entities.pop(entity_id, None)
        if not len(self):
            self.index.inverted.pop(self.token, None)

    def compute(self):
        self.frequencies = {}
        for entity_id, count in self.entities.items():
            terms = self.index.terms.get(entity_id, 0)
            tf = self.weight * count
            self.frequencies[entity_id] = tf / max(terms, 1)

    def tf(self, entity_id):
        """Weighted term frequency for scoring."""
        return self.frequencies.get(entity_id, 0)

    def all_tf(self):
        return self.frequencies.items()

    def __repr__(self):
        return "<IndexEntry(%r, %r)>" % (self.token, self.weight)

    def __len__(self):
        return len(self.entities)

    def to_dict(self):
        return dict(
            token=self.token,
            weight=self.weight,
            entities=self.entities,
        )

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

    def schema_token(self, schema):
        return f"s:{schema.name}"

    def tokenize_entity(self, entity, adjacent=False):
        yield f"d:{entity.dataset.name}", 0.0
        yield self.schema_token(entity.schema), 0.0
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

    # def remove(self, entity):
    #     """Remove an entity from the index."""
    #     self.terms.pop(entity.id, None)
    #     for entry in self.inverted.values():
    #         entry.remove(entity.id)

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
        self.commit()
        log.info("Built index", index=self)

    def commit(self):
        for entry in self.inverted.values():
            entry.compute()

    def _match_schema(self, entity_id, schema):
        tokens = set()
        for matchable in schema.matchable_schemata:
            tokens.add(self.schema_token(matchable))
        for token in tokens:
            entry = self.inverted.get(token)
            if entry is not None and entity_id in entry.entities:
                return True
        return False

    def match(self, query, limit=30):
        """Find entities similar to the given input entity."""
        if not query.schema.matchable:
            return
        matches = {}
        for token, _ in self.tokenize_entity(query):
            entry = self.inverted.get(token)
            if entry is None or len(entry) == 0:
                continue
            idf = math.log(self.num_entities / len(entry))
            for entity_id, tf in entry.all_tf():
                if entity_id == query.id or tf <= 0:
                    continue

                weight = tf * idf
                entity_score = matches.get(entity_id)
                if entity_score == -1:
                    continue
                if entity_score is None:
                    # Filter out incompatible types:
                    if not self._match_schema(entity_id, query.schema):
                        matches[entity_id] = -1
                        continue
                    entity_score = 0
                matches[entity_id] = entity_score + weight

        results = sorted(matches.items(), key=lambda x: x[1], reverse=True)
        results = [(id, score) for (id, score) in results if score > 0]
        log.debug("Match entity", query=query, results=len(results))
        for result_id, score in results[:limit]:
            yield self.loader.get_entity(result_id), score

    def save(self):
        with open(self.get_path(self.dataset.name), "wb") as fh:
            pickle.dump(self, fh)

    @classmethod
    def load(cls, dataset):
        path = cls.get_path(dataset)
        if path.exists():
            with open(path, "rb") as fh:
                index = pickle.load(fh)
                index.commit()
                log.debug("Loaded index", index=index)
                return index
        index = Index(dataset.name)
        index.build()
        index.save()
        return index

    @classmethod
    def get_path(self, dataset):
        index_dir = settings.DATA_PATH.joinpath("index")
        index_dir.mkdir(exist_ok=True)
        return index_dir.joinpath(f"{dataset}.pkl")

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
        self.loader = DBEntityLoader(self.dataset)
        entries = [IndexEntry.from_dict(self, i) for i in state["inverted"]]
        self.inverted = {e.token: e for e in entries}
        self.terms = state.get("terms")

    def __repr__(self):
        return "<Index(%r, %d, %d)>" % (
            self.dataset.name,
            len(self.inverted),
            len(self.terms),
        )
