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

# Country / territory groups
# TODO: expose in rigour
GLOBAL = "zz"

EU_MEMBERS = frozenset(
    "at be bg cy cz de dk ee es fi fr gr hr hu ie it lt lu lv mt nl pl pt ro se si sk".split()
)
EFTA_MEMBERS = frozenset({"ch", "is", "li", "no"})
EUROPEAN_MICROSTATES = frozenset({"ad", "mc", "sm", "va"})
EUROPE_OTHER = frozenset({"al", "ba", "md", "me", "mk", "rs", "ua", "xk"})

UK_AND_CROWN_DEPENDENCIES = frozenset({"gb", "gg", "im", "je"})
