# Checklist when reviewing a crawler

Some things that are easy to forget but critical for new crawlers

- Metadata
  - dataset name is clear and conforms to convention
  - dataset title is concise and menaingful
  - dataset prefix is short yet meaningful
  - publisher country code is correct
- Crawler
  - It emits as many entities as are available at the source
  - It doesn't emit warnings or errors (excpet transient, e.g. network timeout/server error that can be expected to go away tomorrow)