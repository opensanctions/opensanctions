

```sql
SELECT aet.id, aet.caption AS name, aet.schema AS entity_type, ac.country AS linked_country,
	(SELECT MIN(x) FROM jsonb_array_elements_text(aet.properties->'createdAt') AS x) AS created_at,
	(SELECT string_agg(x, ', ') FROM jsonb_array_elements_text(aes.properties->'country') AS x) AS sanctioning_country,
	(SELECT string_agg(x, ', ') FROM jsonb_array_elements_text(aes.properties->'authority') AS x) AS sanctioning_authority,
	(SELECT string_agg(x, ', ') FROM jsonb_array_elements_text(aes.properties->'program') AS x) AS program,
	(SELECT string_agg(x, ', ') FROM jsonb_array_elements_text(aes.properties->'listingDate') AS x) as listing_date,
	(SELECT string_agg(x, ', ') FROM jsonb_array_elements_text(aes.properties->'startDate') AS x) as start_date,
	aes.first_seen as firstSeen
	FROM
		analytics_entity aet,
		analytics_dataset ad,
		analytics_entity aes,
		analytics_country ac,
		statement sa,
		statement sc
	WHERE aes.id = ad.entity_id
		AND ad.dataset = 'sanctions'
		AND aet.id = sc.canonical_id
		AND aet.id = ac.entity_id
		AND sc.prop = 'id'
		AND sc.entity_id = sa.value
		AND sc.dataset <> 'us_bis_denied'
		AND sa.prop = 'entity'
		AND sa.canonical_id = aes.id
		AND aes.schema = 'Sanction'
```