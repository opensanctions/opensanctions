from functools import cache
from itertools import product
import json
import string
import networkx as nx
from pathlib import Path
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver, Linker, Edge
from nomenklatura.resolver.identifier import Identifier
from rigour.ids.wikidata import is_qid
from typing import Dict, List, Optional, Set, Tuple, Generator
from followthemoney.types import registry

from zavod.entity import Entity
from zavod.logs import get_logger, configure_logging
from zavod.meta import get_catalog
from zavod.meta.dataset import Dataset
from zavod.integration import get_resolver
from zavod.store import View, get_store

log = get_logger("sam_merge")
alpha = string.ascii_uppercase + string.digits


@cache
def norm_token(value: str) -> Optional[str]:
    if " " not in value:
        return None
    # value = "".join(c for c in value.upper() if c in alpha)
    if len(value) < 5:
        return None
    return value


def decide_pair(left: Entity, right: Entity) -> bool:
    pass


def dedupe_sam():
    catalog = get_catalog()
    scope = catalog.require("us_sam_exclusions")
    resolver = get_resolver()
    store = get_store(scope, resolver)
    store.sync(clear=True)
    view = store.view(scope)
    ueis: Dict[str, str] = {}
    uei_merges = 0
    all_authorities: Set[str] = set()
    blocking: Dict[str, Set[str]] = {}
    for entity in view.entities():
        for uei in entity.get("uniqueEntityId", quiet=True):
            if uei in ueis:
                log.info(f"Duplicate UEI: {uei}")
                if is_qid(ueis[uei]) and is_qid(entity.id):
                    log.warn("Both are QIDs! %s %s" % (ueis[uei], entity.id))
                    continue

                if not resolver.check_candidate(ueis[uei], entity.id):
                    log.warn("Merge denied: %s %s" % (ueis[uei], entity.id))
                    continue

                canonical = resolver.decide(
                    ueis[uei], entity.id, Judgement.POSITIVE, "auto/sam"
                )
                ueis[uei] = canonical.id
                uei_merges += 1
                continue
            else:
                ueis[uei] = entity.id

        authorities: Set[str] = set()
        for _, adj in view.get_inverted(entity.id):
            if adj.schema.is_a("Sanction"):
                authority = adj.first("authority")
                authorities.add(authority)
                all_authorities.add(authority)

        # if len(authorities) > 1:
        #     log.info(f"Multiple authorities: {entity.id} {authorities}")
        names = entity.get_type_values(registry.name, matchable=True)
        addresses = entity.get_type_values(registry.address, matchable=True)
        for authority, name, address in product(authorities, names, addresses):
            norm_name = norm_token(name)
            norm_address = norm_token(address)
            if norm_name is None or norm_address is None:
                continue
            key = (authority, norm_name, norm_address)
            if key is None:
                continue
            if key not in blocking:
                blocking[key] = set()
            blocking[key].add(entity.id)

    log.info("Merged %s entities based on UEI" % uei_merges)
    # for key, ids in blocking.items():
    #     if len(ids) == 1:
    #         continue
    #     resolver.check_candidate()
    #     log.info(f"Blocking: {key} {ids}")
    resolver.save()


if __name__ == "__main__":
    configure_logging()
    dedupe_sam()
