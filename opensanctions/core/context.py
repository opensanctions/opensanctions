import json
from lxml import etree, html
from nomenklatura.util import normalize_url

from zavod import settings
from zavod.context import Context as ZavodContext
from zavod.meta import Dataset


class Context(ZavodContext):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    def __init__(self, dataset: Dataset, dry_run: bool = False):
        super().__init__(dataset, dry_run=dry_run)

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
