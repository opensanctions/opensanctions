
Targets on the EU sanctions list which are not on the Swiss one: 

```sql
SELECT ae.id AS id, ae.schema AS type, ac.country AS country, ae.caption AS main_name,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthDate') AS x) AS birth_dates,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'notes') AS x) AS notes
	FROM analytics_entity ae, analytics_country ac, analytics_dataset ad
	WHERE
		ae.id = ac.entity_id
		AND ac.country IN ('ru', 'by', 'suhh')
		AND ad.entity_id = ae.id
		AND ad.dataset = 'eu_fsf'
		AND ae.target = true
		AND NOT EXISTS(SELECT 1 FROM statement sch WHERE sch.dataset = 'ch_seco_sanctions' AND sch.target = true AND sch.canonical_id = ae.id)
;
```


```sql
SELECT ae.id AS id, ae.schema AS type, ac.country AS country, ae.caption AS main_name,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthDate') AS x) AS birth_dates,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'notes') AS x) AS notes,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(sanc.properties->'authority') AS x) AS authority,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(sanc.properties->'listingDate') AS x) AS listing_date,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(sanc.properties->'reason') AS x) AS reason,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(sanc.properties->'program') AS x) AS program
	FROM analytics_entity ae, analytics_country ac, analytics_dataset ad, statement sa, statement ss, analytics_entity sanc
	WHERE
		ae.id = ac.entity_id
		AND ac.country IN ('ru', 'by', 'suhh')
		AND ad.entity_id = ae.id
		AND ad.dataset = 'eu_fsf'
		AND sa.canonical_id = ae.id
		AND sa.prop = 'id'
		AND sa.dataset = 'eu_fsf'
		AND ss.value = sa.entity_id
		AND ss.schema = 'Sanction'
		AND ss.prop = 'entity'
		AND ss.dataset = 'eu_fsf'
		AND ae.target = true
		AND sanc.id = ss.canonical_id
		AND NOT EXISTS(SELECT 1 FROM statement sch WHERE sch.dataset = 'ch_seco_sanctions' AND sch.target = true AND sch.canonical_id = ae.id)
;
```