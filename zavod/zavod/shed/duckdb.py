from io import TextIOWrapper
from followthemoney.types import registry
from pathlib import Path
from shutil import rmtree
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple
import csv
import duckdb
import logging

from nomenklatura.dataset import DS
from nomenklatura.entity import CE
from nomenklatura.index.common import BaseIndex
from nomenklatura.index.tokenizer import NAME_PART_FIELD, WORD_FIELD, Tokenizer
from nomenklatura.resolver import Pair, Identifier
from nomenklatura.store import View

type MatchCandidates = List[Tuple[Identifier, float]]

log = logging.getLogger(__name__)

BATCH_SIZE = 1000


class DuckDBIndex(BaseIndex[DS, CE]):
    """
    An index using DuckDB for token matching and scoring, keeping data in memory
    until it needs to spill to disk as it approaches the configured memory limit.

    Pairs match if they share one or more tokens. A basic similarity score is calculated
    cumulatively based on each token's Term Frequency (TF) and the field's boost factor.
    """

    BOOSTS = {
        NAME_PART_FIELD: 2.0,
        WORD_FIELD: 0.5,
        registry.name.name: 10.0,
        # registry.country.name: 1.5,
        # registry.date.name: 1.5,
        # registry.language: 0.7,
        # registry.iban.name: 3.0,
        registry.phone.name: 3.0,
        registry.email.name: 3.0,
        # registry.entity: 0.0,
        # registry.topic: 2.1,
        registry.address.name: 2.5,
        registry.identifier.name: 3.0,
    }

    __slots__ = "view", "fields", "tokenizer", "entities"

    def __init__(
        self, view: View[DS, CE], data_dir: Path, options: Dict[str, Any] = {}
    ):
        self.view = view
        memory_budget = options.get("memory_budget", None)
        self.memory_budget: Optional[int] = (
            int(memory_budget) if memory_budget else None
        )
        """Memory budget in megabytes"""
        self.max_candidates = int(options.get("max_candidates", 50))
        self.tokenizer = Tokenizer[DS, CE]()
        self.data_dir = data_dir
        if self.data_dir.exists():
            rmtree(self.data_dir)
        self.data_dir.mkdir(parents=True)
        self.con = duckdb.connect((self.data_dir / "duckdb_index.db").as_posix())
        self.matching_path = self.data_dir / "matching.csv"
        self.matching_path.unlink(missing_ok=True)
        self.matching_dump: TextIOWrapper | None = open(self.matching_path, "w")
        writer = csv.writer(self.matching_dump)
        writer.writerow(["id", "field", "token"])

        # https://duckdb.org/docs/guides/performance/environment
        # > For ideal performance,
        # > aggregation-heavy workloads require approx. 5 GB memory per thread and
        # > join-heavy workloads require approximately 10 GB memory per thread.
        # > Aim for 5-10 GB memory per thread.
        if self.memory_budget is not None:
            self.con.execute("SET memory_limit = ?;", [f"{self.memory_budget}MB"])
        # > If you have a limited amount of memory, try to limit the number of threads
        self.con.execute("SET threads = 1;")

    def build(self) -> None:
        """Index all entities in the dataset."""
        log.info("Building index from: %r...", self.view)
        self.con.execute("CREATE TABLE boosts (field TEXT, boost FLOAT)")
        for field, boost in self.BOOSTS.items():
            self.con.execute("INSERT INTO boosts VALUES (?, ?)", [field, boost])

        self.con.execute("CREATE TABLE matching (id TEXT, field TEXT, token TEXT)")
        self.con.execute("CREATE TABLE entries (id TEXT, field TEXT, token TEXT)")
        csv_path = self.data_dir / "mentions.csv"
        log.info("Dumping entity tokens to CSV for bulk load into the database...")
        with open(csv_path, "w") as fh:
            writer = csv.writer(fh)

            # csv.writer type gymnastics
            def dump_entity(entity: CE) -> None:
                if not entity.schema.matchable or entity.id is None:
                    return
                for field, token in self.tokenizer.entity(entity):
                    writer.writerow([entity.id, field, token])
                    writer.writerow(["id", "field", "token"])

            for idx, entity in enumerate(self.view.entities()):
                dump_entity(entity)
                if idx % 50000 == 0:
                    log.info("Dumped %s entities" % idx)
        log.info("Loading data...")
        self.con.execute(f"COPY entries from '{csv_path}'")
        log.info("Done.")

        self._build_frequencies()
        log.info("Index built.")

    def _build_field_len(self) -> None:
        self._build_stopwords()
        log.info("Calculating field lengths...")
        field_len_query = """
            CREATE TABLE IF NOT EXISTS field_len as
            SELECT entries.field, entries.id, count(*) as field_len from entries
            LEFT OUTER JOIN stopwords
            ON stopwords.field = entries.field AND stopwords.token = entries.token
            WHERE token_freq is NULL
            GROUP BY entries.field, entries.id
        """
        self.con.execute(field_len_query)

    def _build_mentions(self) -> None:
        self._build_stopwords()
        log.info("Calculating mention counts...")
        mentions_query = """
            CREATE TABLE IF NOT EXISTS mentions as
            SELECT entries.field, entries.id, entries.token, count(*) as mentions
            FROM entries
            LEFT OUTER JOIN stopwords
            ON stopwords.field = entries.field AND stopwords.token = entries.token
            WHERE token_freq is NULL
            GROUP BY entries.field, entries.id, entries.token
        """
        self.con.execute(mentions_query)

    def _build_stopwords(self) -> None:
        token_freq_query = """
            SELECT field, token, count(*) as token_freq
            FROM entries
            GROUP BY field, token
        """
        token_freq = self.con.sql(token_freq_query)  # noqa
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS stopwords as
            SELECT * FROM token_freq where token_freq > 100
            """
        )

    def _build_frequencies(self) -> None:
        self._build_field_len()
        self._build_mentions()
        log.info("Calculating term frequencies...")
        term_frequencies_query = """
            CREATE TABLE IF NOT EXISTS term_frequencies as
            SELECT mentions.field, mentions.token, mentions.id, mentions/field_len as tf
            FROM field_len
            JOIN mentions
            ON field_len.field = mentions.field AND field_len.id = mentions.id
        """
        self.con.execute(term_frequencies_query)

    def pairs(
        self, max_pairs: int = BaseIndex.MAX_PAIRS
    ) -> Iterable[Tuple[Pair, float]]:
        pairs_query = """
            SELECT "left".id, "right".id, sum(("left".tf + "right".tf) * ifnull(boost, 1)) as score
            FROM term_frequencies as "left"
            JOIN term_frequencies as "right"
            ON "left".field = "right".field AND "left".token = "right".token
            LEFT OUTER JOIN boosts
            ON "left".field = boosts.field
              AND "left".id > "right".id
            GROUP BY "left".id, "right".id
            ORDER BY score DESC
            LIMIT ?
        """
        results = self.con.execute(pairs_query, [max_pairs])
        while batch := results.fetchmany(BATCH_SIZE):
            for left, right, score in batch:
                yield (Identifier.get(left), Identifier.get(right)), score

    def add_matching_subject(self, entity: CE) -> None:
        if self.matching_dump is None:
            raise Exception("Cannot add matching subject after getting candidates.")
        writer = csv.writer(self.matching_dump)
        for field, token in self.tokenizer.entity(entity):
            writer.writerow([entity.id, field, token])

    def matches(
        self,
    ) -> Generator[Tuple[Identifier, MatchCandidates], None, None]:
        if self.matching_dump is not None:
            self.matching_dump.close()
            self.matching_dump = None
            log.info("Loading matching subjects...")
            self.con.execute(f"COPY matching from '{self.matching_path}'")
            log.info("Finished loading matching subjects.")

        match_query = """
            SELECT matching.id, matches.id, sum(matches.tf * ifnull(boost, 1)) as score
            FROM term_frequencies as matches
            JOIN matching
            ON matches.field = matching.field AND matches.token = matching.token
            LEFT OUTER JOIN boosts
            ON matches.field = boosts.field
            GROUP BY matches.id, matching.id
            ORDER BY matching.id, score DESC
        """
        results = self.con.execute(match_query)
        previous_id = None
        matches: MatchCandidates = []
        while batch := results.fetchmany(BATCH_SIZE):
            for matching_id, match_id, score in batch:
                # first row
                if previous_id is None:
                    previous_id = matching_id
                # Next pair of subject and candidates
                if matching_id != previous_id:
                    if matches:
                        yield Identifier.get(previous_id), matches
                    matches = []
                    previous_id = matching_id
                matches.append((Identifier.get(match_id), score))
        # Last pair or subject and candidates
        if matches and previous_id is not None:
            yield Identifier.get(previous_id), matches[: self.max_candidates]

    def __repr__(self) -> str:
        return "<DuckDBIndex(%r, %r)>" % (
            self.view.scope.name,
            self.con,
        )
