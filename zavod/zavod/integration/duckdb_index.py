import csv
import duckdb
import logging
from io import TextIOWrapper
from pathlib import Path
from shutil import rmtree
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple
from followthemoney.types import registry
from nomenklatura.dataset import DS
from nomenklatura.entity import CE
from nomenklatura.index.common import BaseIndex
from nomenklatura.resolver import Pair, Identifier
from nomenklatura.store import View

from zavod.integration.tokenizer import tokenize_entity
from zavod.integration.tokenizer import NAME_PART_FIELD, WORD_FIELD, PHONETIC_FIELD

BlockingMatches = List[Tuple[Identifier, float]]

log = logging.getLogger(__name__)

BATCH_SIZE = 10000


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
        self.stopwords_pct: float = float(options.get("stopwords_pct", 0.8))
        self.data_dir = data_dir.resolve()
        if self.data_dir.exists():
            rmtree(self.data_dir)
        self.data_dir.mkdir(parents=True)
        data_file = self.data_dir / "index.duckdb"
        tmp_dir = self.data_dir / "index.duckdb.tmp"
        config = {
            "preserve_insertion_order": False,
            # > If you have a limited amount of memory, try to limit the number of threads
            "threads": 1,
            "temp_directory": tmp_dir.as_posix(),
        }
        if self.memory_budget is not None:
            config["memory_limit"] = f"{self.memory_budget}MB"
            config["max_memory"] = f"{self.memory_budget}MB"
        self.con = duckdb.connect(data_file.as_posix(), config=config)
        self.matching_path = self.data_dir / "matching.csv"
        self.matching_path.unlink(missing_ok=True)
        self.matching_dump: TextIOWrapper | None = open(self.matching_path, "w")
        writer = csv_writer(self.matching_dump)
        writer.writerow(["id", "field", "token"])

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
            "CREATE OR REPLACE TABLE matching (id TEXT, field TEXT, token TEXT)"
        )
        self.con.execute(
            "CREATE OR REPLACE TABLE entries (id TEXT, field TEXT, token TEXT)"
        )
        csv_path = self.data_dir / "mentions.csv"
        log.info("Dumping entity tokens to CSV for bulk load into the database...")
        with open(csv_path, "w") as fh:
            writer = csv_writer(fh)
            for idx, entity in enumerate(self.view.entities()):
                if not entity.schema.matchable or entity.id is None:
                    continue
                for field, token in tokenize_entity(entity):
                    writer.writerow([entity.id, field, token])

                if idx % 50000 == 0 and idx > 0:
                    log.info("Dumped %s entities" % idx)

        log.info("Loading data...")
        self.con.execute(
            f"COPY entries FROM '{csv_path}' (HEADER false, AUTO_DETECT false, ESCAPE '\\')"
        )
        log.info("Done.")

        self._build_frequencies()
        log.info("Index built.")

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
            SELECT entries.field, entries.id, entries.token, count(*) AS mentions
            FROM entries
            LEFT OUTER JOIN stopwords
            ON stopwords.field = entries.field AND stopwords.token = entries.token
            WHERE stopwords.freq is NULL
            GROUP BY entries.field, entries.id, entries.token
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
        num_tokens_results = self.con.execute("SELECT count(*) FROM tokens").fetchone()
        assert num_tokens_results is not None
        num_tokens = num_tokens_results[0]
        limit = int((num_tokens / 100) * self.stopwords_pct)
        log.info(
            "Treating %d (%s%%) most common tokens as stopwords...",
            limit,
            self.stopwords_pct,
        )
        self.con.execute(
            """
            CREATE OR REPLACE TABLE stopwords as
            SELECT * FROM tokens ORDER BY freq DESC LIMIT ?;
            """,
            [limit],
        )
        least_common_query = """
            SELECT field, token, freq
            FROM stopwords
            ORDER BY freq ASC
            LIMIT 5;
        """
        least_common = "\n".join(
            f"{freq} {field}:{token}"
            for field, token, freq in self.con.sql(least_common_query).fetchall()
        )
        log.info("5 Least common stopwords:\n%s\n", least_common)

    def _build_frequencies(self) -> None:
        self._build_stopwords()
        self._build_field_len()
        self._build_mentions()
        log.info("Calculating term frequencies...")
        term_frequencies_query = """
            CREATE OR REPLACE TABLE term_frequencies AS
            SELECT mentions.field, mentions.token, mentions.id, (mentions/field_len) * ifnull(boo.boost, 1) as tf
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
        writer = csv_writer(self.matching_dump)
        for field, token in tokenize_entity(entity):
            writer.writerow([entity.id, field, token])

    def matches(
        self,
    ) -> Generator[Tuple[Identifier, BlockingMatches], None, None]:
        if self.matching_dump is not None:
            self.matching_dump.close()
            self.matching_dump = None
            log.info("Loading matching subjects...")
            self.con.execute(
                f"COPY matching FROM '{self.matching_path}' (HEADER false, AUTO_DETECT false, ESCAPE '\\')"
            )
            log.info("Finished loading matching subjects.")

        self.con.execute("CHECKPOINT")
        self.con.execute("ANALYZE matching")

        match_table_query = """
        CREATE OR REPLACE TABLE agg_matches AS
            -- Create chunks in the matching table using window functions
            WITH chunked_matching AS (
                SELECT id as matching_id,
                    field,
                    token,
                    ntile(100) OVER (ORDER BY field, token) as chunk_id
                FROM matching
            ),
            -- Process each chunk separately
            processed_chunks AS (
                SELECT m.matching_id AS matching_id,
                    tf.id AS matches_id,
                    sum(tf.tf) as score
                FROM chunked_matching m
                JOIN term_frequencies tf 
                    ON m.field = tf.field 
                    AND m.token = tf.token
                GROUP BY matching_id, matches_id
            )
            -- Insert results into our temporary table
            SELECT matching_id, matches_id, score
            FROM processed_chunks;
        """
        self.con.execute(match_table_query)
        log.info("Finished match generation, now returning scored pairs...")
        match_query = """
        SELECT * FROM agg_matches ORDER BY matching_id, score DESC;
        """
        results = self.con.execute(match_query)
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

    def close(self) -> None:
        self.con.close()
        if self.matching_dump is not None:
            self.matching_dump.close()

    def __repr__(self) -> str:
        return "<DuckDBIndex(%r, %r)>" % (
            self.view.scope.name,
            self.con,
        )
