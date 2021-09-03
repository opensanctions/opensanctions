# pip install whoosh
import shutil
from whoosh import index
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED, NGRAMWORDS
from whoosh.analysis import StemmingAnalyzer
from followthemoney.types import registry
from opensanctions import settings
from opensanctions.core import Dataset
from opensanctions.exporters.index import ExportIndex

dataset = Dataset.get("un_sc_sanctions")
cache = ExportIndex(dataset)
print("INDEX DATASETS")


schema = Schema(
    id=ID(stored=True),
    name=NGRAMWORDS(stored=False, minsize=2, maxsize=4, field_boost=2.0, queryor=True),
    # text=TEXT(analyzer=StemmingAnalyzer()),
    schema=KEYWORD(stored=False),
    schemata=KEYWORD(stored=False),
    data=STORED(),
)


index_dir = settings.DATA_PATH.joinpath("index")
shutil.rmtree(index_dir)
index_dir.mkdir(exist_ok=True)

ix = index.create_in(index_dir.as_posix(), schema)

writer = ix.writer()
for entity in cache.entities.values():
    if not entity.target:
        continue
    # print(entity)
    names = entity.get_type_values(registry.name)
    writer.add_document(
        id=entity.id,
        name=" ".join(names),
        schema=entity.schema.name,
        schemata=" ".join(entity.schema.names),
        data=entity.to_dict(),
    )

writer.commit()

searcher = ix.searcher()
# print(list(searcher.lexicon('name')))

from whoosh.qparser import QueryParser

qp = QueryParser("name", schema=ix.schema)
q = qp.parse(u"kevin")
# q = qp.parse(u"schemata:LegalEntity")

results = searcher.search(q, limit=1000)
for result in results:
    # entity = cache.get_entity(result.get('id'))
    # print(result, repr(entity), entity.schema)

    print(result.get("data"))
