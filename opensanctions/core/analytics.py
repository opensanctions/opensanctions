from typing import Any, Dict, List
from sqlalchemy.sql.expression import delete, insert
from followthemoney.types import registry

from opensanctions.core.logs import get_logger
from opensanctions.core.db import engine_tx, analytics_country_table
from opensanctions.core.db import analytics_dataset_table, analytics_entity_table
from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import Database
from opensanctions.core.resolver import get_resolver
from opensanctions.core.statements import resolve_all_canonical

log = get_logger(__name__)
BATCH_SIZE = 5000


def build_analytics(dataset: Dataset):
    resolver = get_resolver()
    with engine_tx() as conn:
        resolve_all_canonical(conn, resolver)
    db = Database(dataset, resolver)
    loader = db.view(dataset)
    with engine_tx() as conn:
        conn.execute(delete(analytics_dataset_table))
        conn.execute(delete(analytics_country_table))
        conn.execute(delete(analytics_entity_table))

        entities: List[Dict[str, Any]] = []
        members: List[Dict[str, str]] = []
        countries: List[Dict[str, str]] = []
        for idx, entity in enumerate(loader):
            if idx > 0 and idx % 10000 == 0:
                log.info("Denormalised %d entities..." % idx)

            for dataset in Dataset.all():
                if len(entity.datasets.intersection(dataset.scope_names)) > 0:
                    members.append({"entity_id": entity.id, "dataset": dataset.name})

            if len(members) >= BATCH_SIZE:
                stmt = insert(analytics_dataset_table).values(members)
                conn.execute(stmt)
                members = []

            for country in entity.get_type_values(registry.country):
                countries.append({"entity_id": entity.id, "country": country})

            if len(countries) >= BATCH_SIZE:
                stmt = insert(analytics_country_table).values(countries)
                conn.execute(stmt)
                countries = []

            ent = {
                "id": entity.id,
                "schema": entity.schema.name,
                "caption": entity.caption,
                "target": entity.target,
                "first_seen": entity.first_seen,
                "last_seen": entity.last_seen,
                "properties": entity.properties,
            }
            entities.append(ent)

            if len(entities) >= BATCH_SIZE:
                stmt = insert(analytics_entity_table).values(entities)
                conn.execute(stmt)
                entities = []

        if len(members):
            conn.execute(insert(analytics_dataset_table).values(members))

        if len(entities):
            conn.execute(insert(analytics_entity_table).values(entities))
