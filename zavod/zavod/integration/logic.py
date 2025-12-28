from typing import List, Optional
from rigour.names import prenormalize_name
from followthemoney import Schema, model, registry
from nomenklatura import Resolver, Judgement

from zavod.logs import get_logger
from zavod.entity import Entity

log = get_logger(__name__)
USER = "zavod/logic"
UNIQUE_DATASETS = {"us_ofac_sdn", "eu_fsf", "un_sc_sanctions"}


def logic_unique(
    resolver: Resolver[Entity],
    common: Schema,
    left: Entity,
    right: Entity,
    score: float,
) -> Optional[float]:
    """We consider the legal entities on the given unique lists to be, de jure, different."""
    if common.is_a("Address"):
        return score
    both = left.datasets.intersection(right.datasets)
    uniques = both.intersection(UNIQUE_DATASETS)
    if len(uniques) > 0:
        if left.id is not None and right.id is not None:
            scope = "|".join(sorted(uniques))
            log.info(f"{scope} negative match: {left.id} <> {right.id}")
            resolver.decide(left.id, right.id, Judgement.NEGATIVE, user=USER)
            return None
    return score


def logic_vessel_match(
    resolver: Resolver[Entity],
    common: Schema,
    left: Entity,
    right: Entity,
    score: float,
) -> Optional[float]:
    """Custom logic for matching vessels based on IMO number."""
    if not common.is_a("Vessel"):
        return score
    left_imo = set(left.get("imoNumber"))
    right_imo = set(right.get("imoNumber"))
    if not len(left_imo.intersection(right_imo)) > 0:
        return score
    left_names_ = left.get_type_values(registry.name, matchable=True)
    left_names = set(prenormalize_name(n) for n in left_names_)
    right_names_ = right.get_type_values(registry.name, matchable=True)
    right_names = set(prenormalize_name(n) for n in right_names_)
    if not len(left_names.intersection(right_names)) > 0:
        return score
    if left.id is not None and right.id is not None:
        log.info("Vessel positive match: %s %s" % (left.id, right.id))
        resolver.decide(left.id, right.id, Judgement.POSITIVE, user=USER)
    return score


def logic_securities_mismatch(
    resolver: Resolver[Entity],
    common: Schema,
    left: Entity,
    right: Entity,
    score: float,
) -> Optional[float]:
    """Custom logic for avoiding matches between securities with different ISINs."""
    if not common.is_a("Security"):
        return score
    left_isin = set(left.get("isin"))
    right_isin = set(right.get("isin"))
    if len(left_isin) > 0 and len(right_isin) > 0:
        if len(left_isin.intersection(right_isin)) == 0:
            if left.id is not None and right.id is not None:
                # log.info("Security ISIN negative match: %s %s" % (left.id, right.id))
                # resolver.decide(left.id, right.id, Judgement.NEGATIVE, user=USER)
                return None
    return score


def _perfect_identifier_match(left: List[str], right: List[str]) -> bool:
    left_set = set(left)
    right_set = set(right)
    longest = max(len(left_set), len(right_set))
    if longest == 0:
        return False
    return len(left_set.intersection(right_set)) == longest


def logic_identifiers(
    resolver: Resolver[Entity],
    common: Schema,
    left: Entity,
    right: Entity,
    score: float,
) -> Optional[float]:
    """Custom logic for avoiding matches between Russian entities with different identifiers."""
    if not common.is_a("LegalEntity") or left.id is None or right.id is None:
        return score
    left_countries = set(left.get_type_values(registry.country, matchable=True))
    right_countries = set(right.get_type_values(registry.country, matchable=True))
    overlap = left_countries.intersection(right_countries)
    if "ru" in overlap and len(left_countries) == 1 and len(right_countries) == 1:
        if common.is_a("Person"):
            left_inns = left.get("innCode")
            right_inns = right.get("innCode")
            if _perfect_identifier_match(left_inns, right_inns):
                log.info("Russian INN match: %s %s" % (left.id, right.id))
                resolver.decide(left.id, right.id, Judgement.POSITIVE, user=USER)
                return None
        if common.is_a("Organization"):
            left_ogrns = left.get("ogrnCode")
            right_ogrns = right.get("ogrnCode")
            if _perfect_identifier_match(left_ogrns, right_ogrns):
                log.info("Russian OGRN match: %s %s" % (left.id, right.id))
                resolver.decide(left.id, right.id, Judgement.POSITIVE, user=USER)
                return None
    if common.is_a("Organization"):
        for unique_prop in {"leiCode", "imoNumber"}:
            left_vals = left.get(unique_prop, quiet=True)
            right_vals = right.get(unique_prop, quiet=True)
            if _perfect_identifier_match(left_vals, right_vals):
                log.info(f"Organization {unique_prop} match: {left.id} <> {right.id}")
                resolver.decide(left.id, right.id, Judgement.POSITIVE, user=USER)
                return None
    return score


def logic_pkpro_ids(
    resolver: Resolver[Entity],
    common: Schema,
    left: Entity,
    right: Entity,
    score: float,
) -> Optional[float]:
    """Custom logic for avoiding matches between entities from Pakistan."""
    if not common.is_a("Person"):
        return score
    PK = "pk_proscribed_persons"
    if PK not in left.datasets or PK not in right.datasets:
        return score
    left_ids_ = left.get_statements("idNumber")
    left_ids = set([s.value for s in left_ids_ if s.dataset == PK])
    right_ids_ = right.get_statements("idNumber")
    right_ids = set([s.value for s in right_ids_ if s.dataset == PK])
    if len(left_ids) > 0 and len(right_ids) > 0 and left_ids.isdisjoint(right_ids):
        if left.id is not None and right.id is not None:
            log.info("PK proscribed negative match: %s %s" % (left.id, right.id))
            resolver.decide(left.id, right.id, Judgement.NEGATIVE, user=USER)
            return None
    return score


def logic_decide(
    resolver: Resolver[Entity], left: Entity, right: Entity, score: float
) -> Optional[float]:
    """Decide whether to automatically merge two entities based on custom logic."""
    common = model.common_schema(left.schema, right.schema)
    res_score = logic_vessel_match(resolver, common, left, right, score)
    if res_score is None:
        return None
    res_score = logic_identifiers(resolver, common, left, right, res_score)
    if res_score is None:
        return None
    res_score = logic_unique(resolver, common, left, right, res_score)
    if res_score is None:
        return None
    res_score = logic_pkpro_ids(resolver, common, left, right, res_score)
    if res_score is None:
        return None
    res_score = logic_securities_mismatch(resolver, common, left, right, res_score)
    if res_score is None:
        return None
    return res_score
