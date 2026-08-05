# Values from the crawler or program metadata:
ORIGIN_METADATA = "metadata"

# Values inferred by the crawler based on other data (eg. country parsing, name composition)
ORIGIN_INFERRED = "inferred"

# Coming from data patch lookups
ORIGIN_LOOKUP = "patch"

# Datasets that emit derived annotations (topic patches, PEP categorisation)
# computed from the rest of the graph, rather than crawled source data. Their
# statements don't count as substance when deciding whether an entity has
# published source data, and entities consisting only of their statements are
# skipped as enrichment subjects. Hard-coded for now; could become a dataset
# metadata flag (`type: analyzer`) if the list grows.
ANALYZER_DATASETS = frozenset({"ann_graph_topics", "ann_pep_positions"})
