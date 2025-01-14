# HTTP Operations

## Request Management
Use context methods for HTTP operations, as they handle common tasks such as caching, using request sessions with sensible retry defaults, and checking the response status.

```python
response = context.fetch_text(context.data_url, method="POST", headers=headers, data=body, cache_days=cache_days)
```

## Request Headers
If you have to override the user-agent, include identifying information that allows the host to recognize us as the client. This helps them notify us if our requests are negatively impacting their server. Additionally, it's preferable to use the metadata HTTP configuration to override the user-agent when necessary.

```python
HEADERS = { "Accept": "application/json", "User-Agent": "opensanctions.org" }
```


## Handling 403 Errors with Zyte
If adding a browser-like `User-Agent` header (and additional headers) does not resolve a `403 Forbidden` error, switch to using Zyte for scraping. Zyte is specifically designed to bypass anti-scraping measures that commonly block typical scraping requests by simulating real user behavior.