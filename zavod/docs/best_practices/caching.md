# Caching Considerations

## General Caching Guidelines
- We don’t cache sanctions index pages so that we can discover new entities as quickly as possible - we sometimes crawl these more than once a day.

- If we cache an index page, we usually don’t cache it for more than 1 day so that retries on a day are likely to succeed, but tomorrow we’ll get a fresh crawl.

- For slow-moving sources like PEP databases, we might cache detail pages (e.g. a profile of a person) longer, especially for bigger sites with thousands of pages, and if absolute freshness isn’t critical. The tradeoff is discovering new things vs completing crawls.

- It’s nice to discover a minister’s new position, but it’s okay if it takes a few days if we already have them in the system.

- We’d like to finish a crawl of a big site with many HTTP errors or hundreds of thousands of pages. Caching helps reduce the number of pages requested each crawl.

!!! info "Please note"
    It’s critical to discover newly sanctioned persons, and important to discover new 
    high-level PEPs very quickly - hence selectively or never caching index/listing pages.

## Issue with Pagination and Cached Pages
When new content is added to a website, it can cause issues with how the pages are cached. For example, if new items push existing ones to a different page, the cached version of the page will not show the updated content immediately. This happens because the scraper continues to use the old version of the page until it's refreshed, causing some data to disappear and reappear later.

**Solution: Avoid Caching on Paginated Pages.** It’s best to not cache paginated pages (those where we request page 1, page 2, etc.) because they can change dynamically when entities are added or removed.