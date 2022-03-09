
```sql
SELECT aet.id, aet.caption AS name, aet.schema AS entity_type, sa.dataset AS dataset,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(aes.properties->'country') AS x) AS sanctioning_country,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(aes.properties->'authority') AS x) AS sanctioning_authority,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(aes.properties->'program') AS x) AS program,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(aes.properties->'listingDate') AS x) as listing_date,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(aes.properties->'startDate') AS x) as start_date,
	aes.first_seen as firstSeen
	FROM
		analytics_entity aet,
		analytics_dataset ad,
		analytics_entity aes,
		statement sa,
		statement sc
	WHERE aes.id = ad.entity_id
		AND ad.dataset = 'sanctions'
		AND aet.id = sc.canonical_id
		AND sc.prop = 'id'
		AND sc.entity_id = sa.value
		AND sc.dataset <> 'us_bis_denied'
        AND sc.dataset <> 'us_trade_csl'
		AND sa.prop = 'entity'
		AND sa.canonical_id = aes.id
		AND aes.schema = 'Sanction';
```


```sql
SELECT aet.id, aet.caption AS name, aet.schema AS entity_type,
       sa.dataset AS dataset, sa.first_seen AS first_seen,
       sn.value AS notes
	FROM
		analytics_entity aet,
		analytics_dataset ad,
		statement sa
    LEFT OUTER JOIN statement sn ON sn.entity_id = sa.entity_id AND sn.prop = 'notes'
	WHERE aet.id = ad.entity_id
		AND ad.dataset = 'sanctions'
		AND aet.id = sa.canonical_id
		AND sa.prop = 'id'
		AND sa.dataset <> 'us_bis_denied'
        AND sa.dataset <> 'us_trade_csl'
        AND sa.dataset <> 'ar_repet'
        AND sa.dataset <> 'kz_afmrk_sanctions'
        AND sa.dataset <> 'interpol_red_notice'
        AND sa.dataset <> 'ca_listed_terrorists'
		AND sa.first_seen > '2022-01-01'
        AND sa.target = true;
```

```
SELECT aet.id, aet.caption AS name, aet.schema AS entity_type,
       sa.dataset AS dataset, sa.first_seen AS first_seen
	FROM
		analytics_entity aet,
		analytics_dataset ad,
		statement sa
	WHERE aet.id = ad.entity_id
		AND ad.dataset = 'sanctions'
		AND aet.id = sa.canonical_id
		AND sa.prop = 'id'
		AND sa.dataset <> 'us_bis_denied'
        AND sa.dataset <> 'us_trade_csl'
        AND sa.dataset <> 'ar_repet'
        AND sa.dataset <> 'kz_afmrk_sanctions'
        AND sa.dataset <> 'interpol_red_notice'
        AND sa.dataset <> 'ca_listed_terrorists'
		AND sa.first_seen > '2022-01-01'
        AND sa.target = true;
```