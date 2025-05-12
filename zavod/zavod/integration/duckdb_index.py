import csv
import logging
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple

import duckdb
from followthemoney import model
from followthemoney.types import registry
from nomenklatura.dataset import DS
from nomenklatura.entity import CE
from nomenklatura.index.common import BaseIndex
from nomenklatura.resolver import Identifier, Pair
from nomenklatura.store import View
from rigour.ids.wikidata import is_qid

from zavod.integration.tokenizer import (
    NAME_PART_FIELD,
    PHONETIC_FIELD,
    WORD_FIELD,
    tokenize_entity,
)
from zavod.reset import reset_caches

BlockingMatches = List[Tuple[Identifier, float]]

log = logging.getLogger(__name__)

BATCH_SIZE = 10000
DEFAULT_STOPWORDS_PCT = 0.8
DEFAULT_FIELD_STOPWORDS_PCT = {
    registry.phone.name: 0.0,
    registry.identifier.name: 0.0,
    registry.country.name: 80.0,
    PHONETIC_FIELD: 10.0,
    WORD_FIELD: 10.0,
}


def can_match(
    left_id: str,
    left_schema_name: str,
    right_id: str,
    right_schema_name: str,
) -> bool:
    if is_qid(left_id) and is_qid(right_id) and left_id != right_id:
        return False
    left_schema = model.get(left_schema_name)
    right_schema = model.get(right_schema_name)
    if left_schema is None or right_schema is None:
        return False
    return left_schema.can_match(right_schema)


def csv_writer(
    fh: TextIOWrapper,
) -> Any:  # Any because csv writer types seem to be special
    return csv.writer(
        fh,
        dialect=csv.unix_dialect,
        escapechar="\\",
        doublequote=False,
    )


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
        PHONETIC_FIELD: 2.0,
        registry.name.name: 10.0,
        registry.phone.name: 3.0,
        registry.email.name: 3.0,
        registry.address.name: 2.5,
        registry.identifier.name: 6.0,
    }

    __slots__ = "view", "fields", "tokenizer", "entities"

    def __init__(
        self, view: View[DS, CE], data_dir: Path, options: Dict[str, Any] = {}
    ):
        self.view = view
        memory_budget = options.get("memory_budget")
        # https://duckdb.org/docs/guides/performance/environment
        # > For ideal performance,
        # > aggregation-heavy workloads require approx. 5 GB memory per thread and
        # > join-heavy workloads require approximately 10 GB memory per thread.
        # > Aim for 5-10 GB memory per thread.
        self.memory_budget: Optional[int] = (
            int(memory_budget) if memory_budget else None
        )
        """Memory budget in megabytes"""
        self.max_candidates = int(options.get("max_candidates", 75))
        self.stopwords_pct = DEFAULT_FIELD_STOPWORDS_PCT
        self.stopwords_pct.update(options.get("stopwords_pct", {}))
        self.max_stopwords: int = int(options.get("max_stopwords", 100_000))
        self.match_batch: int = int(options.get("match_batch", 1_000))
        self.data_dir = data_dir.resolve()
        # if self.data_dir.exists():
        #     rmtree(self.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.duckdb_config = {
            "preserve_insertion_order": False,
            # > If you have a limited amount of memory, try to limit the number of threads
            "threads": 1,
            "temp_directory": (self.data_dir / "index.duckdb.tmp").as_posix(),
        }
        if self.memory_budget is not None:
            self.duckdb_config["memory_limit"] = f"{self.memory_budget}MB"
            self.duckdb_config["max_memory"] = f"{self.memory_budget}MB"
        self.duckdb_path = (self.data_dir / "index.duckdb").as_posix()

        self.matching_path = self.data_dir / "matching.csv"

        self._init_db()

    def _init_db(self) -> None:
        self.con = duckdb.connect(self.duckdb_path, config=self.duckdb_config)
        self.con.create_function("can_match", can_match)

    def _clear(self) -> None:
        self.con.execute("CHECKPOINT")
        self.con.close()
        self._init_db()

    def build(self) -> None:
        """Index all entities in the dataset."""
        log.info("Building index from: %r...", self.view)
        self.con.execute("CREATE OR REPLACE TABLE boosts (field TEXT, boost FLOAT)")
        for field, boost in self.BOOSTS.items():
            self.con.execute("INSERT INTO boosts VALUES (?, ?)", [field, boost])
        for type in registry.types:
            if type.name in self.BOOSTS:
                continue
            self.con.execute("INSERT INTO boosts VALUES (?, ?)", [type.name, 1.0])

        self.con.execute(
            "CREATE OR REPLACE TABLE entries (schema TEXT, id TEXT, field TEXT, token TEXT)"
        )
        csv_path = self.data_dir / "mentions.csv"
        log.info("Dumping entity tokens to CSV for bulk load into the database...")
        with open(csv_path, "w") as fh:
            writer = csv_writer(fh)
            for idx, entity in enumerate(self.view.entities()):
                if not entity.schema.matchable or entity.id is None:
                    continue
                self.dump_entity(writer, entity)

                if idx % 50000 == 0 and idx > 0:
                    log.info("Dumped %s entities" % idx)

        log.info("Loading data...")
        self.con.execute(
            f"COPY entries FROM '{csv_path}' (HEADER false, AUTO_DETECT false, ESCAPE '\\')"
        )
        log.info("Done.")

        self._build_frequencies()
        log.info("Index built.")

    def dump_entity(self, writer: Any, entity: CE) -> None:
        for field, token in tokenize_entity(entity):
            writer.writerow([entity.schema.name, entity.id, field, token])

    def load_matching_subjects(self, entities: Iterable[CE]) -> None:
        self.matching_path.unlink(missing_ok=True)
        with open(self.matching_path, "w") as matching_dump:
            matching_writer = csv_writer(matching_dump)
            for entity in entities:
                self.dump_entity(matching_writer, entity)

        log.info("Loading matching subjects...")
        self.con.execute(
            "CREATE OR REPLACE TABLE matching (schema TEXT, id TEXT, field TEXT, token TEXT)"
        )
        self.con.execute(
            f"COPY matching FROM '{self.matching_path}' (HEADER false, AUTO_DETECT false, ESCAPE '\\')"
        )
        log.info("Finished loading matching subjects.")

    def _build_field_len(self) -> None:
        log.info("Calculating field lengths...")
        field_len_query = """
        CREATE OR REPLACE TABLE field_len as
            SELECT entries.field, entries.id, count(*) as field_len FROM entries
            LEFT OUTER JOIN stopwords
            ON stopwords.field = entries.field AND stopwords.token = entries.token
            WHERE stopwords.freq is NULL
            GROUP BY entries.field, entries.id
        """
        self.con.execute(field_len_query)

    def _build_mentions(self) -> None:
        log.info("Calculating mention counts...")
        mentions_query = """
        CREATE OR REPLACE TABLE mentions as
            SELECT entries.schema, entries.field, entries.id, entries.token, count(*) AS mentions
            FROM entries
            LEFT OUTER JOIN stopwords
            ON stopwords.field = entries.field AND stopwords.token = entries.token
            WHERE stopwords.freq is NULL
            GROUP BY entries.schema, entries.field, entries.id, entries.token
        """
        self.con.execute(mentions_query)

    def _build_stopwords(self) -> None:
        token_freq_query = """
        CREATE OR REPLACE TABLE tokens AS
            SELECT field, token, count(*) as freq
            FROM entries
            GROUP BY field, token
        """
        self.con.execute(token_freq_query)
        self.con.execute(
            "CREATE OR REPLACE TABLE stopwords (field TEXT, token TEXT, freq INT)"
        )
        field_counts = self.con.execute(
            "SELECT field, count(*) FROM tokens GROUP BY field"
        ).fetchall()
        for field, count in field_counts:
            self.build_field_stopwords(field, count)

    def build_field_stopwords(self, field: str, num_tokens: int) -> None:
        field_stopwords_pct = self.stopwords_pct.get(field, DEFAULT_STOPWORDS_PCT)
        if field_stopwords_pct == 0.0:
            log.info("Stopwords disabled for field '%s'.", field)
            return
        limit = int((num_tokens / 100) * field_stopwords_pct)
        limit = min(limit, self.max_stopwords)
        log.info(
            "Treating %d (%s%%) most common tokens as stopwords for field '%s'...",
            limit,
            field_stopwords_pct,
            field,
        )
        self.con.execute(
            """
            INSERT INTO stopwords
            SELECT * FROM tokens WHERE field = ? ORDER BY freq DESC LIMIT ?;
            """,
            [field, limit],
        )
        least_common_query = """
            SELECT field, token, freq
            FROM stopwords
            WHERE field = ?
            ORDER BY freq ASC
            LIMIT 5;
        """
        least_common = "\n".join(
            f"{freq} {field}:{token}"
            for field, token, freq in self.con.execute(
                least_common_query, [field]
            ).fetchall()
        )
        log.info("5 Least common stopwords for field '%s':\n%s\n", field, least_common)

    def _build_frequencies(self) -> None:
        self._build_stopwords()
        self._build_field_len()
        self._build_mentions()
        log.info("Calculating term frequencies...")
        term_frequencies_query = """
            CREATE OR REPLACE TABLE term_frequencies AS
            SELECT mentions.schema, mentions.field, mentions.token, mentions.id, (mentions/field_len) * ifnull(boo.boost, 1) as tf
            FROM field_len
            JOIN mentions
            ON field_len.field = mentions.field AND field_len.id = mentions.id
            LEFT OUTER JOIN boosts boo
            ON field_len.field = boo.field
        """
        self.con.execute(term_frequencies_query)

    def pairs(
        self, max_pairs: int = BaseIndex.MAX_PAIRS
    ) -> Iterable[Tuple[Pair, float]]:
        pairs_query = """
            SELECT "left".id, "right".id, sum(("left".tf + "right".tf)) as score
            FROM term_frequencies as "left"
            JOIN term_frequencies as "right"
            ON "left".field = "right".field AND "left".token = "right".token
            WHERE "left".id > "right".id
            AND can_match("left".id, "left".schema, "right".id, "right".schema)
            GROUP BY "left".id, "right".id
            ORDER BY score DESC
            LIMIT ?
        """
        results = self.con.execute(pairs_query, [max_pairs])
        while batch := results.fetchmany(BATCH_SIZE):
            for left, right, score in batch:
                yield (Identifier.get(left), Identifier.get(right)), score

    def match_entities(
        self, entities: Iterable[CE]
    ) -> Generator[Tuple[Identifier, BlockingMatches], None, None]:
        self.load_matching_subjects(entities)
        reset_caches()
        yield from self.matches()

    def matches(self) -> Generator[Tuple[Identifier, BlockingMatches], None, None]:
        self._clear()
        res = self.con.execute("SELECT COUNT(DISTINCT id) FROM matching").fetchone()
        num_matching = res[0] if res is not None else 0
        chunks = max(1, num_matching // self.match_batch)

        chunk_table_query = """
        CREATE OR REPLACE TABLE matching_chunks AS
            WITH ids AS (SELECT DISTINCT id FROM matching)
            SELECT id, ntile(?) OVER (ORDER BY id) as chunk FROM ids
        """
        self.con.execute(chunk_table_query, [chunks])
        self._clear()

        log.info("Matching %d entities in %d chunks...", num_matching, chunks)
        for chunk in range(1, chunks + 1):
            chunk_query = """
            SELECT m.id AS matching_id, tf.id AS matches_id, SUM(tf.tf) AS score
                FROM matching_chunks c
                JOIN matching m ON c.id = m.id
                JOIN term_frequencies tf
                ON m.field = tf.field AND m.token = tf.token
                WHERE c.chunk = ?
                AND can_match(m.id, m.schema, tf.id, tf.schema)
                GROUP BY m.id, tf.id
                ORDER BY m.id, score DESC
            """
            results = self.con.execute(chunk_query, [chunk])
            previous_id = None
            matches: BlockingMatches = []
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
                    if len(matches) <= self.max_candidates:
                        matches.append((Identifier.get(match_id), score))
            # Last pair or subject and candidates
            if matches and previous_id is not None:
                yield Identifier.get(previous_id), matches[: self.max_candidates]
                # yield Identifier.get(previous_id), matches
            self._clear()

    def close(self) -> None:
        self.con.close()

    def __repr__(self) -> str:
        return "<DuckDBIndex(%r, %r)>" % (
            self.view.scope.name,
            self.con,
        )
