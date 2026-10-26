# Understanding the source

Before writing a crawler:

- Search for API documentation, developer portals and data catalogues, both the
  publishing institution's and the country's.
- Search GitHub for the domain name or the publisher's name: existing crawlers often show
  how to access the data. Use them to learn the access path, and write the crawler to
  zavod conventions rather than porting their code.
- Look for a JSON or XML endpoint behind the rendered page (page source, network
  requests, JavaScript bundles) before parsing HTML.
- Use the authoritative source, published by the institution itself. Ignore civil
  society projects that republish the data, even with a cleaner API: including them is
  legally complex for us.
- Make sure the data is still being updated. Some APIs were stopped at some point and
  contain only historical data; find where the institution publishes now.
- If requests are blocked, try a browser `user_agent`, a language cookie or
  `Accept-Language` header, and a format suffix (`.json`) or `Accept:` header before
  [the Zyte API](http_operations.md#the-zyte-api).
- Read the pagination contract from the response (page size cap, total or `next` link):
  an API that silently caps the page size truncates without error.
