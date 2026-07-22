## Enrichment

https://nomenklatura.followthemoney.tech/reference/enrich/

https://www.opensanctions.org/docs/enrichment/

Enrichment can add entities related to entities with one of the enricher's filter topics, e.g. a company they own, the board of directors, etc. This is only done for related entities that have also been tagged with a filter topic. It can also add "supporting" schemata, e.g. linked Article entities.

Topic-based expansion works as follows:

1. First an enricher emits the adjacent entities (one hop over edge entities) as external.
2. ann_graph_topics loads both internal and external entities, and emits the patch with the topic as external for adjacent entities that are still only external.
3. Next the enricher sees the (external) risk topic, and now emits the adjacent entity as internal.
4. Finally ann_graph_topics emits the patch as internal because it now sees the adjacent entity has become internal.

Edges (Ownership, Family, Documentation) etc are emitted when both vertices are "publishable" (have a filter topic or is supporting).
