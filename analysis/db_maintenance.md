

### Delete the oldest cached enrichment items

```sql
DELETE FROM cache WHERE key IN (SELECT key FROM cache WHERE key ILIKE 'https://externals.opensanctions.org/%' ORDER BY timestamp ASC LIMIT 20000);
```
