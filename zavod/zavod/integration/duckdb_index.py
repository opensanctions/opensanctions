# Test realistic memory usage with constrained memory because duckdb only spills
# when it's constrained, but you don't know how much more memory it will use than
# the memory limit.
# e.g. docker build . --tag opensanctions and
# docker run -ti --name xref \
#            -v ./data:/data
#            -v .:/opensanctions \
#            --memory 3G
#            -e ZAVOD_DATA_PATH=/data
#            -e ZAVOD_DATABASE_URI=postgresql://postgres:password@host.docker.internal:5432/dev
#            opensanctions bash
#
# Debugging memory usage?
# https://duckdb.org/docs/stable/guides/troubleshooting/oom_errors
#
# Most DuckDB operations spill to disk when its memory_limit setting is reached.
#
# DuckDB uses more memory than its memory_limit setting.
# > This limit only applies to the buffer manager.
# https://duckdb.org/docs/1.2/operations_manual/limits
#
# Beyond that, other things in zavod also use memory.
#
# When duckdb's memory_limit is reached and it cannot spill to disk,
# it will throw an error like duckdb.duckdb.OutOfMemoryException:
# Out of Memory Error: could not allocate block of size 30.5 MiB (32.8 MiB/47.6 MiB used)
#
# When the process is killed due to the operating system running out of memory,
# making memory_limit smaller might help fit what the buffer manager manages,
# plus all the additional DuckDB and non-DuckDB memory usage.

from collections import defaultdict
import csv
import logging
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple

import duckdb
from followthemoney import model, registry, DS, SE
from nomenklatura.index.common import BaseIndex
from nomenklatura.resolver import Identifier
from nomenklatura.store import View

from zavod.integration.tokenizer import (
    NAME_PART_FIELD,
    PHONETIC_FIELD,
    WORD_FIELD,
    tokenize_entity,
)
from zavod.reset import reset_caches
from zavod import settings

BlockingMatches = List[Tuple[Identifier, float]]

log = logging.getLogger(__name__)

BATCH_SIZE = 10000
# Reducing these increases memory usage
DEFAULT_STOPWORDS_PCT = 0.8
DEFAULT_FIELD_STOPWORDS_PCT = {
    registry.name.name: 0.2,
    registry.phone.name: 0.0,
    registry.identifier.name: 0.0,
    registry.country.name: 85.0,
    registry.address.name: 10.0,
    PHONETIC_FIELD: 20.0,
    WORD_FIELD: 15.0,
    NAME_PART_FIELD: 4.0,
}


class DuckDBIndex(BaseIndex[DS, SE]):
    """
    An index using DuckDB for token matching and scoring, keeping data in memory
    until it needs to spill to disk as it approaches the configured memory limit.

    Pairs match if they share one or more tokens. A basic similarity score is calculated
    cumulatively based on each token's Term Frequency (TF) and the field's boost factor.
    """

    BOOSTS = {
        NAME_PART_FIELD: 5.0,
        WORD_FIELD: 0.5,
        PHONETIC_FIELD: 2.0,
        registry.name.name: 15.0,
        registry.phone.name: 10.0,
        registry.email.name: 10.0,
        registry.address.name: 1.0,
        registry.identifier.name: 10.0,
    }

    __slots__ = "view", "fields", "tokenizer", "entities"

    def __init__(
        self, view: View[DS, SE], data_dir: Path, options: Dict[str, Any] = {}
    ):
        self.view = view
        memory_budget = options.get("memory_budget", settings.XREF_MEMORY)
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
        self.stopwords_pct = DEFAULT_FIELD_STOPWORDS_PCT.copy()
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
            "threads": int(settings.XREF_THREADS),
            "temp_directory": (self.data_dir / "index.duckdb.tmp").as_posix(),
        }
        if self.memory_budget is not None:
            self.duckdb_config["memory_limit"] = f"{self.memory_budget}MB"
            self.duckdb_config["max_memory"] = f"{self.memory_budget}MB"
        self.duckdb_path = (self.data_dir / "index.duckdb").as_posix()
        self._init_db()

    def _init_db(self) -> None:
        self.con = duckdb.connect(self.duckdb_path, config=self.duckdb_config)

    def _clear(self) -> None:
        self.con.execute("CHECKPOINT")
        self.con.close()
        self._init_db()

    def load_entities(self, table: str, entities: Iterable[SE]) -> None:
        path = self.data_dir / f"{table}.csv"
        log.info("Dumping tokenized entities to CSV: %r", table)
        with open(path, "w") as fh:
            writer = csv.writer(
                fh,
                dialect=csv.unix_dialect,
                escapechar="\\",
                doublequote=False,
            )
            idx = 0
            for entity in entities:
                if not entity.schema.matchable or entity.id is None:
                    continue
                counts: Dict[Tuple[str, str], int] = defaultdict(int)
                for field, token in tokenize_entity(entity):
                    counts[(field, token)] += 1

                for (field, token), count in counts.items():
                    row = [entity.schema.name, entity.id, field, token, count]
                    writer.writerow(row)

                idx += 1
                if idx % 50000 == 0:
                    log.info("Dumped %s entities" % idx)
        self.con.execute(f"""
        CREATE OR REPLACE TABLE {table}
            (schema TEXT, id TEXT, field TEXT, token TEXT, count INT)
        """)
        log.info("Loading data to table %r...", table)
        q = f"COPY {table} FROM '{path}' (HEADER false, AUTO_DETECT false, ESCAPE '\\')"
        self.con.execute(q)

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

        q = """CREATE OR REPLACE TABLE schemata ("left" TEXT, "right" TEXT)"""
        self.con.execute(q)
        for left in model.schemata.values():
            for right in left.matchable_schemata:
                q = "INSERT INTO schemata VALUES (?, ?)"
                self.con.execute(q, [left.name, right.name])

        self.load_entities("entries", self.view.entities())
        self._build_frequencies()
        log.info("Index built.")

    def _build_stopwords(self) -> None:
        token_freq_query = """
        CREATE OR REPLACE TABLE tokens AS
            SELECT field, token, sum("count") as freq
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
            SELECT token, freq
            FROM stopwords
            WHERE field = ?
            ORDER BY freq ASC
            LIMIT 5;
        """
        least_common = "\n".join(
            f"{freq} {token}"
            for token, freq in self.con.execute(least_common_query, [field]).fetchall()
        )
        log.info("5 Least common stopwords for field '%s':\n%s\n", field, least_common)

    def _apply_stopwords(self, origin_table: str, target_table: str) -> None:
        log.info("Filtering stopwords from %r, as %r...", origin_table, target_table)
        q = f"""
        CREATE OR REPLACE TABLE {target_table} as
            SELECT e.*
            FROM {origin_table} AS e
            LEFT OUTER JOIN stopwords ON stopwords.token = e.token
            WHERE stopwords.token is NULL
        """
        self.con.execute(q)

    def _build_frequencies(self) -> None:
        self._build_stopwords()
        self._apply_stopwords("entries", "entries_filtered")
        log.info("Calculating term frequencies...")
        term_frequencies_query = """
        CREATE OR REPLACE TABLE term_frequencies AS
            WITH field_len AS (
                SELECT e.field, e.id, sum(e.count) as len
                    FROM entries_filtered e
                    GROUP BY e.field, e.id
            )
            SELECT e.schema, e.field, e.token, e.id, (e.count/f.len) * ifnull(boo.boost, 1) as tf
            FROM entries_filtered AS e
            JOIN field_len AS f ON f.field = e.field AND f.id = e.id
            LEFT OUTER JOIN boosts boo ON f.field = boo.field
        """
        self.con.execute(term_frequencies_query)

    def pairs(
        self, max_pairs: int = BaseIndex.MAX_PAIRS
    ) -> Iterable[Tuple[Tuple[Identifier, Identifier], float]]:
        log.info("Generating pairs...")
        pairs_query = """
            SELECT "left".id, "right".id, sum(("left".tf + "right".tf)) as score
            FROM term_frequencies as "left"
            JOIN term_frequencies as "right" ON "left".token = "right".token
            INNER JOIN schemata ON schemata.left = "left".schema AND schemata.right = "right".schema
            WHERE "left".id > "right".id
            GROUP BY "left".id, "right".id
            ORDER BY score DESC
            LIMIT ?
        """
        results = self.con.execute(pairs_query, [max_pairs])
        while batch := results.fetchmany(BATCH_SIZE):
            for left, right, score in batch:
                yield (Identifier.get(left), Identifier.get(right)), score

    def match_entities(
        self, entities: Iterable[SE]
    ) -> Generator[
        Tuple[Identifier, BlockingMatches],
        None,
        None,
    ]:
        self.load_entities("matching", entities)
        self._apply_stopwords("matching", "matching_filtered")
        reset_caches()
        yield from self._find_matches()

    def _find_matches(
        self,
    ) -> Generator[
        Tuple[Identifier, BlockingMatches],
        None,
        None,
    ]:
        self._clear()
        q = "SELECT COUNT(DISTINCT id) FROM matching_filtered"
        res = self.con.execute(q).fetchone()
        num_matching = res[0] if res is not None else 0
        chunks = max(1, num_matching // self.match_batch)

        chunk_table_query = """
        CREATE OR REPLACE TABLE matching_chunks AS
            WITH ids AS (SELECT DISTINCT id FROM matching_filtered)
            SELECT id, ntile(?) OVER (ORDER BY id) as chunk FROM ids
        """
        self.con.execute(chunk_table_query, [chunks])
        self._clear()

        log.info("Matching %d entities in %d chunks...", num_matching, chunks)
        for chunk in range(1, chunks + 1):
            chunk_query = """
            SELECT m.id AS matching_id, tf.id AS matches_id, SUM(tf.tf) AS score
                FROM matching_chunks c
                JOIN matching_filtered m ON c.id = m.id
                JOIN term_frequencies tf
                ON m.token = tf.token
                INNER JOIN schemata s
                ON s.left = m.schema AND s.right = tf.schema
                WHERE c.chunk = ?
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
