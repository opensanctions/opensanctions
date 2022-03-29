
For Sarunas:

```sql
SELECT ae.id AS id, ae.schema AS type, ac.country AS country, ae.caption AS main_name, sn.value AS name,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthDate') AS x) AS birth_dates
	FROM analytics_entity ae, analytics_country ac, analytics_dataset ad, statement sn WHERE ae.id = ac.entity_id AND ac.country IN ('ru', 'by') AND ad.entity_id = ae.id AND ad.dataset = 'eu_fsf' AND ae.schema IN ('Person', 'LegalEntity', 'Organization', 'Company') AND sn.canonical_id = ae.id AND sn.prop_type = 'name';
```


```sql
SELECT ae.id AS id, ae.schema AS type, ac.country AS country, ae.caption AS main_name,
	sanc.first_seen AS sanction_date,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'name') AS x) AS other_names,
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
		AND ad.dataset = 'sanctions'
		AND sa.canonical_id = ae.id
		AND sa.prop = 'id'
		AND ss.value = sa.entity_id
		AND ss.schema = 'Sanction'
		AND ss.prop = 'entity'
		AND ae.target = true
		AND sanc.id = ss.canonical_id
		AND sanc.first_seen > '2022-02-20'
	ORDER BY sanc.first_seen DESC, ae.id
;
```