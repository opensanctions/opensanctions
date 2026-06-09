import csv
import hashlib
import io
import logging
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple

from followthemoney import DS, SE
from nomenklatura.cache import Cache
from nomenklatura.enrich.common import EnricherConfig
from nomenklatura.enrich.common import EnrichmentAbort, EnrichmentException
from nomenklatura.enrich.permid import PermIDEnricher as NomenklaturaPermIDEnricher
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

log = logging.getLogger(__name__)

CSV_HEADER = ["LocalID", "Standard Identifier", "Name", "Country"]
DEFAULT_BATCH_SIZE = 999


class PermIDEnricher(NomenklaturaPermIDEnricher):
    def __init__(
        self,
        dataset: DS,
        cache: Cache,
        config: EnricherConfig,
        session: Optional[Session] = None,
    ):
        super().__init__(dataset, cache, config, session=session)
        batch_size = self.get_config_int("batch_size", DEFAULT_BATCH_SIZE)
        self.batch_size = max(1, min(batch_size, DEFAULT_BATCH_SIZE))

    def entity_to_query_rows(self, entity: SE) -> List[List[str]]:
        csv_data = self.entity_to_queries(entity).decode("utf-8")
        reader = csv.reader(io.StringIO(csv_data), dialect=csv.unix_dialect)
        rows = list(reader)
        return rows[1:]

    def rows_to_query(self, rows: List[List[str]]) -> bytes:
        sio = io.StringIO()
        writer = csv.writer(sio, dialect=csv.unix_dialect, delimiter=",")
        writer.writerow(CSV_HEADER)
        writer.writerows(rows)
        sio.seek(0)
        return sio.getvalue().encode("utf-8")

    def match_headers(self) -> Dict[str, str]:
        headers = {
            "x-openmatch-numberOfMatchesPerRecord": "4",
            "x-openmatch-dataType": "Organization",
        }
        if self.api_token is not None:
            headers["X-AG-Access-Token"] = self.api_token
        return headers

    @staticmethod
    def api_response_message(response: Response) -> Optional[str]:
        try:
            data = response.json()
        except ValueError:
            text = response.text.strip()
            return text[:1000] if len(text) else None
        if isinstance(data, dict):
            for key in ("message", "error_description", "error"):
                message = data.get(key)
                if message is not None:
                    return str(message)
        return str(data)[:1000] if data is not None else None

    def log_match_api_error(self, response: Response) -> None:
        message = self.api_response_message(response)
        log.debug(
            "PermID match API error [%s]: %s (retry-after=%s)",
            response.status_code,
            message,
            response.headers.get("Retry-After"),
        )

    @contextmanager
    def match_api_retries(self) -> Generator[None, None, None]:
        """Do not let urllib3 sleep on PermID quota responses before we can log them."""
        adapters: List[Tuple[HTTPAdapter, Any]] = []
        for prefix in ("https://", "http://"):
            adapter = self.session.adapters.get(prefix)
            if adapter is None:
                continue
            retry = adapter.max_retries
            adapters.append((adapter, retry))
            status_forcelist = tuple(
                status for status in (retry.status_forcelist or ()) if status != 429
            )
            adapter.max_retries = retry.new(
                status_forcelist=status_forcelist,
                respect_retry_after_header=False,
            )
        try:
            yield
        finally:
            for adapter, retry in adapters:
                adapter.max_retries = retry

    def fetch_match_batch(self, query: bytes) -> Dict[str, Any]:
        cache_key = "permid:batch:%s" % hashlib.sha1(query).hexdigest()
        resp_data = self.cache.get_json(cache_key, max_age=self.cache_days)
        if resp_data is None:
            try:
                with self.match_api_retries():
                    resp = self.session.post(
                        self.MATCHING_API,
                        data=query,
                        headers=self.match_headers(),
                    )
                resp.raise_for_status()
            except RequestException as exc:
                response = exc.response
                if response is not None:
                    self.log_match_api_error(response)
                    if response.status_code in (401, 403, 429):
                        message = self.api_response_message(response) or str(exc)
                        raise EnrichmentAbort(
                            "HTTP %s: %s" % (response.status_code, message)
                        ) from exc
                raise EnrichmentException(
                    "HTTP POST failed [%s]: %s" % (self.MATCHING_API, exc)
                ) from exc
            resp_data = resp.json()
            if self.cache_days > 0:
                self.cache.set_json(cache_key, resp_data)
        return resp_data

    def direct_matches(
        self, entity: SE, seen_matches: Set[str]
    ) -> Generator[SE, None, None]:
        for permid in entity.get("permId", quiet=True):
            permid_url = f"https://permid.org/1-{permid}"
            if permid_url in seen_matches:
                continue
            seen_matches.add(permid_url)
            match = self.fetch_perm_org(entity, permid_url)
            if match is not None:
                yield match

    def result_local_id(
        self, result: Dict[str, Any], entities_by_id: Dict[str, SE]
    ) -> Optional[str]:
        local_id = result.get("Input_LocalID") or result.get("Input LocalID")
        if local_id is not None:
            return str(local_id)
        if len(entities_by_id) == 1:
            return next(iter(entities_by_id))
        return None

    def match_batch(
        self,
        entities_by_id: Dict[str, SE],
        rows: List[List[str]],
        seen_matches: Dict[str, Set[str]],
    ) -> Generator[Tuple[SE, SE], None, None]:
        if not len(rows):
            return
        try:
            res = self.fetch_match_batch(self.rows_to_query(rows))
        except EnrichmentException as exc:
            log.info("PermID match batch failed: %s", exc)
            return

        for result in res.get("outputContentResponse", []):
            local_id = self.result_local_id(result, entities_by_id)
            if local_id is None:
                continue
            entity = entities_by_id.get(local_id)
            if entity is None:
                continue

            match_permid_url = result.get("Match OpenPermID")
            if not match_permid_url or match_permid_url in seen_matches[local_id]:
                continue
            seen_matches[local_id].add(match_permid_url)

            try:
                match = self.fetch_perm_org(entity, match_permid_url)
            except EnrichmentException as exc:
                log.info("PermID record fetch failed [%s]: %s", match_permid_url, exc)
                continue
            if match is not None:
                yield entity, match

    def match_many_wrapped(
        self, entities: Iterable[SE]
    ) -> Generator[Tuple[SE, SE], None, None]:
        if self.quota_exceeded:
            return

        batch_rows: List[List[str]] = []
        batch_entities: Dict[str, SE] = {}
        seen_matches: Dict[str, Set[str]] = defaultdict(set)

        def flush_batch() -> Generator[Tuple[SE, SE], None, None]:
            nonlocal batch_rows, batch_entities
            yield from self.match_batch(batch_entities, batch_rows, seen_matches)
            batch_rows = []
            batch_entities = {}

        try:
            for entity_idx, entity in enumerate(entities):
                if not self._filter_entity(entity):
                    continue
                if not entity.schema.is_a("Organization"):
                    continue

                local_id = entity.id or f"row-{entity_idx}"
                batch_entities[local_id] = entity
                try:
                    yield from (
                        (entity, match)
                        for match in self.direct_matches(entity, seen_matches[local_id])
                    )
                except EnrichmentException as exc:
                    log.info("PermID direct lookup failed [%s]: %s", local_id, exc)

                for row in self.entity_to_query_rows(entity):
                    if len(batch_rows) >= self.batch_size:
                        yield from flush_batch()
                    row = list(row)
                    row[0] = local_id
                    batch_rows.append(row)
                    batch_entities[local_id] = entity

            if len(batch_rows):
                yield from flush_batch()
        except EnrichmentAbort as exc:
            self.quota_exceeded = True
            log.warning("PermID quota exceeded: %s", exc)
