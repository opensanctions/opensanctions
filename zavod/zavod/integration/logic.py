from typing import Optional
from rigour.names import prenormalize_name
from followthemoney import Schema, model, registry
from nomenklatura import Resolver, Judgement

from zavod.logs import get_logger
from zavod.entity import Entity

log = get_logger(__name__)
USER = "zavod/logic"


def logic_unique(
    resolver: Resolver[Entity],
    common: Schema,
    left: Entity,
    right: Entity,
    score: float,
) -> Optional[float]:
    """We consider the legal entities on the US OFAC SDN to be de jure different."""
    if not common.is_a("LegalEntity"):
        return score
    if "us_ofac_sdn" in left.datasets and "us_ofac_sdn" in right.datasets:
        if left.id is not None and right.id is not None:
            log.info("OFAC negative match: %s %s" % (left.id, right.id))
            resolver.decide(left.id, right.id, Judgement.NEGATIVE, user=USER)
            return None
    # TODO: should this be true of UN SC as well?
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
                log.info("Security ISIN negative match: %s %s" % (left.id, right.id))
                resolver.decide(left.id, right.id, Judgement.NEGATIVE, user=USER)
                return None
    return score


def logic_decide(
    resolver: Resolver[Entity], left: Entity, right: Entity, score: float
) -> Optional[float]:
    """Decide whether to automatically merge two entities based on custom logic."""
    res_score: Optional[float] = score
    common = model.common_schema(left.schema, right.schema)
    res_score = logic_unique(resolver, common, left, right, res_score)
    if res_score is None:
        return None
    res_score = logic_vessel_match(resolver, common, left, right, res_score)
    if res_score is None:
        return None
    res_score = logic_securities_mismatch(resolver, common, left, right, res_score)
    if res_score is None:
        return None
    return res_score
