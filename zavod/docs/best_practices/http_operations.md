# HTTP Operations

## Request Management
Use `context` methods for HTTP operations, as they handle common tasks such as caching, using request sessions with sensible retry defaults, and checking the response status.

```python
response = context.fetch_text(context.data_url, method="POST", headers=headers, data=body, cache_days=cache_days)
```

## Handling bot blocking

Many sites employ bot blocking strategies. We believe this is primarily to mitigate Denial of Service attacks and manage server load, rather than protecting the content from extraction, since the purpose of the sites we scrape is dissemination of their block lists. As long as we are sensitive to our impact on their service and identifiable in their requests, we believe it is ok to work around their bot blocking strategies.

Blocking might result in error statuses like 403; redirects to error pages; or 200 status responses but with different content from what you've seen in the browser.

### Header-based restrictions  

If a request using `zavod` fails but your browser succeeds, try making a request with more typical headers. Set a more browser-like user-agent header.  
```yaml
http:
user_agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36 (zavod; opensanctions.org)
```

If that doesn't work, try more of the common headers sent by browsers:  

```python  
HEADERS = {  
    "origin": "https://www.interpol.int",  
    "referer": "https://www.interpol.int/",  
    "sec-fetch-mode": "navigate",  
    "sec-fetch-site": "none",  
    "sec-fetch-user": "?1",  
    "upgrade-insecure-requests": "1",  
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 (zavod; opensanctions.org)",  
}  
```
### Network blocking

If it fails in production but not locally, they might be blocking our production network IP range. It's common to block hosting provider networks for websites intended for humans only.

Use zyte with with `httpResponseBody` approach (default in `zavod.shed.zyte_api.fetch_*` functions except `fetch_htm` whose `html_source` defaults to `browser_html`). `httpResponseBody` is faster and cheaper than `browserHtml`.

It's also common to block requests from a country other than the publisher. If it works using a VPN exit point in that country, also try zyte using the `geolocation` argument.

### JavaScript challenges

If it works in the browser but you see different content when fetching using `zavod` or `curl`, there might be a javascript challenge that checks whether a full browser is rendering the page. This usually sets a cookie so the browser doesn't have to complete the challenge on each request. These challenges can also be intermittent.

For HTML, try requesting using `zyte_api.fetch_text` with `html_source="browserHtml"` (the default). This will render the page in a browser, execute any javascript, then turn the DOM back into HTML and return that.

If tricks like waiting for specific content or clicking on something to render the data is needed, look at the `actions` argument.