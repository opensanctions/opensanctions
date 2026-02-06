import os
import logging
from typing import Literal, Set, Tuple
from anthropic import Anthropic, BaseModel

from followthemoney import registry
from nomenklatura import Judgement, Resolver
from normality import squash_spaces
from zavod.logs import configure_logging, get_logger
from zavod.integration import get_resolver
from zavod.meta import get_catalog, get_multi_dataset, Dataset
from zavod.store import get_store
from zavod.entity import Entity

WHOAMI = "toa/claude"

log = get_logger("team-of-analysts")
logging.getLogger("anthropic").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
ignore: Set[Tuple[str, str]] = set()
catalog = get_catalog()


class AnalystResponse(BaseModel):
    judgement: Literal["positive", "negative", "unsure"]
    short_explanation: str


SYSTEM_PROMPT = """
You are an experienced data analyst working on a entity resolution project. You are presented with pairs of entity
profiles with descriptive properties. Your task is to determine whether the two profiles likely refer to the same
real-world entity or not. Your decisions are conservative and based on the entity descriptions and some common sense
and contextual knowledge. Choose for `unsure` if there is not enough information to make a confident decision.
Consider the following guidelines when making your decision:

* Two entities occurring in the same data source make a match less likely. Some sources have many duplicates, e.g. 
  `ua_nsdc_sanctions` (Person only), `us_cia_world_leaders` (Person only), `us_sam_exclusions` (poor quality overall).
  `ann_pep_positions` and `ann_graph_topics` are not subject to this rule.
* Name properties like `name`, `alias`, `previousName` can cross-match.
* Country properties like `country`, `jurisdiction`, `citizenship`, `nationality` can cross-match.
* Precise properties (eg. `ogrnCode`, `innCode`) can match more generic properties (eg. `registrationNumber`, 'taxNumber`).
* Distinguish different entities in the same corporate group - subsidiaries, siblings, parent companies. If a 
  corporate hierarchy is ambiguous, pick `unsure`.
* The lists `eu_fsf`, `eu_journal_sanctions`, `be_fod_sanctions`, `mc_fund_freezes` contain the same entities, so
  matches across these lists are likely.
* The lists `us_trade_csl`, `us_bis_denied`, `us_ddtc_debarred`, `us_special_leg`, `us_chinese_milcorps` contain the
  same entities, so matches across these lists are likely. Do not merge different subsidiaries of large concerns.
* Sanctions lists like `gb_fcdo_sanctions` and `ch_seco_sanctions` sometimes contain textual references, where only
  a name is given to identify another entity on the same list or other major sanctions lists. These can be merged.
* The lists `us_cia_world_leaders`, `us_cia_world_factbook` and `un_ga_protocol` describe senior national figures, so
  matches across these lists are very likely. One individual often holds multiple ministerial positions.
* Data entry is often inconsistent, so minor differences in names or other properties do not necessarily indicate
  different entities. Birth dates can get severely mangled.
* Consider name frequency in the population (or in the context of the countries identified).
* Political positions from different countries MUST NOT be merged, even if they have the same title. If a Position has
  no country property, only merge it with other positions if the name is specific (e.g. "President of the United States").
* Politicial positions where one is a bundle of posts ("Prime Minister and Minister of Foreign Affairs") and the
  other is a single post ("Prime Minister") MUST NOT be merged. Be precise when matching political positions.
* In contested territories (e.g. Crimea, Donetsk, Luhansk), country properties are less reliable.
* Many profiles in Wikidata (IDs starting with Q) describe people not linked to politics and sanctions. Wikidata profiles
  mentioning "research" are bulk-uploaded authors of scientific papers.
"""


def get_system_prompt() -> str:
    prompt = SYSTEM_PROMPT
    prompt += "\n\nHere are all of the datasets included in the database:\n"
    for dataset in catalog.datasets:
        prompt += f"\n\t\t - {dataset.name}: {dataset.model.title}"
    return prompt


def entity_to_text(entity: Entity) -> str:
    text = f"\n\t<Entity ID: {entity.id}>\n"
    text += f"\t<Schema/EntityType: {entity.schema.name} "
    text += f"(compatible: {', '.join(parent.name for parent in entity.schema.matchable_schemata)}) />\n"
    text += "\t<Properties>\n"
    for prop in entity.schema.sorted_properties:
        if prop.type == registry.entity:
            continue
        values = entity.get(prop)
        if len(values) == 0:
            continue
        value_str = "; ".join([squash_spaces(v) for v in values])
        text += f"\t\t{prop.name} ({prop.type.name}): {value_str}\n"
    text += "\t</Properties>\n"
    text += "\n\t<DataSources>"
    text += f"\n\t\t{', '.join(entity.datasets)}"
    text += "\n\t</DataSources>\n\t</Entity>\n"
    # TODO: expand wikipedia article text for QID entities.
    return text


def decide_pair(resolver: Resolver, left: Entity, right: Entity, score: float) -> str:
    # prompt the team of analysts
    left_str = entity_to_text(left)
    right_str = entity_to_text(right)
    message = f"<left_entity>{left_str}</left_entity>\n\n<right_entity>{right_str}</right_entity>\n\n<score>{score}</score>"
    # print(message)
    print("\n\n=== DECIDING PAIR ===")
    print(f"Left entity [{left.id}]: {left.caption} {left.datasets!r}")
    print(f"Right entity [{right.id}]: {right.caption} {right.datasets!r}")
    print(f"Score: {score}")

    response = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": get_system_prompt(),
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": message}],
        tools=[
            {
                "name": "return_decision",
                "description": "Return the entity resolution decision and a short explanation.",
                "input_schema": AnalystResponse.model_json_schema(),
            }
        ],
        tool_choice={"type": "tool", "name": "return_decision"},
    )
    tool_use = next(block for block in response.content if block.type == "tool_use")
    result = AnalystResponse.model_validate(tool_use.input)

    print(f"\n\nDecision: {result.judgement}")
    print(f"Explanation: {result.short_explanation}")
    # print(f"Usage: {response.usage.input_tokens}, {response.usage.output_tokens}")
    # print(f"Cache create: {response.usage.cache_creation_input_tokens}")
    # print(f"Cache read: {response.usage.cache_read_input_tokens}")
    return result.judgement


def auto_resolve(dataset: Dataset) -> None:
    resolver = get_resolver()
    resolver.begin()
    store = get_store(dataset, resolver)
    view = store.view(dataset, external=True)
    decisions = 0
    for left_id, right_id, score in resolver.get_candidates():
        if decisions % 10 == 0 and decisions > 0:
            log.info(f"Processed {decisions} decisions...")
            resolver.commit()
            resolver.begin()
        # if score is not None and score > 0.3:
        #     continue
        try:
            log.info(f"Resolving pair: {left_id} - {right_id} (score: {score})")
            left_id = resolver.get_canonical(left_id)
            right_id = resolver.get_canonical(right_id)
            if (left_id, right_id) in ignore:
                continue
            if score is None:
                ignore.add((left_id, right_id))
                continue
            if not resolver.check_candidate(left_id, right_id):
                ignore.add((left_id, right_id))
                continue
            left = view.get_entity(left_id)
            right = view.get_entity(right_id)
            if left is None or right is None:
                ignore.add((left_id, right_id))
                continue
            judgement_str = decide_pair(resolver, left, right, score)
            judgement = Judgement(judgement_str)
            # if judgement == Judgement.UNSURE:
            #     continue
            decisions += 1
            id = resolver.decide(left_id, right_id, judgement, user=WHOAMI)
            store.update(id)
        except KeyboardInterrupt:
            log.info("Interrupted by user, exiting.")
            break
    resolver.commit()


if __name__ == "__main__":
    configure_logging()
    page = client.models.list()
    sources = ["peps"]
    dataset = get_multi_dataset(sources)
    auto_resolve(dataset)
