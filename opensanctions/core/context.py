import json
from lxml import etree, html
from sqlalchemy.exc import OperationalError
from requests.exceptions import RequestException
from datapatch import LookupException
from nomenklatura.util import normalize_url

from zavod import settings
from zavod.context import Context as ZavodContext
from zavod.meta import Dataset
from zavod.runtime.loader import load_entry_point


class Context(ZavodContext):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    def __init__(self, dataset: Dataset, dry_run: bool = False):
        super().__init__(dataset, dry_run=dry_run)

    @property
    def source(self) -> Dataset:
        if self.dataset.data is not None:
            return self.dataset
        raise RuntimeError("Dataset is not a source: %s" % self.dataset.name)

    def fetch_response(self, url, headers=None, auth=None):
        self.log.debug("HTTP GET", url=url)
        timeout = (settings.HTTP_TIMEOUT, settings.HTTP_TIMEOUT)
        response = self.http.get(
            url,
            headers=headers,
            auth=auth,
            timeout=timeout,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response

    def fetch_text(
        self,
        url,
        params=None,
        headers=None,
        auth=None,
        cache_days=None,
    ):
        url = normalize_url(url, params)
        if cache_days is not None:
            text = self.cache.get(url, max_age=cache_days)
            if text is not None:
                self.log.debug("HTTP cache hit", url=url)
                return text

        response = self.fetch_response(url, headers=headers, auth=auth)
        text = response.text
        if text is None:
            return None

        if cache_days is not None:
            self.cache.set(url, text)
        return text

    def fetch_json(self, *args, **kwargs):
        """Fetch the given URL (GET) and decode it as a JSON object."""
        text = self.fetch_text(*args, **kwargs)
        if text is not None and len(text):
            return json.loads(text)

    def fetch_html(self, *args, **kwargs):
        text = self.fetch_text(*args, **kwargs)
        if text is not None and len(text):
            return html.fromstring(text)

    def parse_resource_xml(self, name):
        """Parse a file in the resource folder into an XML tree."""
        file_path = self.get_resource_path(name)
        with open(file_path, "rb") as fh:
            return etree.parse(fh)

    def crawl(self) -> bool:
        """Run the crawler."""
        if self.dataset.disabled:
            self.log.info("Dataset is disabled")
            return True
        self.begin(clear=True)
        self.log.info("Begin crawl", run_time=settings.RUN_TIME_ISO)
        try:
            # Run the dataset:
            method = load_entry_point(self.dataset)
            method(self)
            if self.stats.entities == 0:
                self.log.warn(
                    "Crawler did not emit entities",
                    statements=self.stats.statements,
                )
            self.log.info(
                "Crawl completed",
                entities=self.stats.entities,
                statements=self.stats.statements,
            )
            return True
        except KeyboardInterrupt:
            self.log.warning("Aborted by user (SIGINT)")
            return False
        except LookupException as lexc:
            self.log.error(lexc.message, lookup=lexc.lookup.name, value=lexc.value)
            return False
        except OperationalError as oexc:
            self.log.error("Database error: %r" % oexc)
            return False
        except RequestException as rexc:
            resp = repr(rexc.response)
            self.log.error(str(rexc), url=rexc.request.url, response=resp)
            return False
        except Exception as exc:
            self.log.exception("Crawl failed", error=str(exc))
            raise
        finally:
            self.close()

    def clear(self, data: bool = True) -> None:
        """Delete all recorded data for a given dataset."""
        self.issues.clear()
        self.resources.clear()
        if data:
            self.cache.clear()
            self.sink.clear()
