

```sql
SELECT 
	ae.id AS entity_id,
	ae.caption AS main_name,
	sc.value AS country,
	sn.value AS name_variant,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthDate') AS x) AS birth_date,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthPlace') AS x) AS birth_place,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'position') AS x) AS "position"
	FROM statement sn, statement sc, analytics_entity ae LEFT JOIN analytics_dataset ad ON ae.id = ad.entity_id
	WHERE
		ad.dataset = 'peps'
		AND sn.canonical_id = ae.id
		AND sc.canonical_id = ae.id
		AND sc.value IN ('be', 'el', 'lt', 'pt', 'bg', 'es', 'lu', 'ro', 'cz', 'fr', 'hu', 'si', 'dk', 'hr', 'mt', 'sk', 'de', 'it', 'nl', 'fi', 'ee', 'cy', 'at', 'se', 'ie', 'lv', 'pl', 'gb')
		AND sn.prop_type = 'name'
		AND sc.prop_type = 'country'
		AND ae.schema IN ('Person', 'Organization', 'Company', 'LegalEntity');
```


```sql
SELECT 
	ae.id AS entity_id,
	ae.caption AS main_name,
	sc.value AS country,
	sn.value AS name_variant,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthDate') AS x) AS birth_date,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthPlace') AS x) AS birth_place,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'position') AS x) AS "position"
	FROM statement sn, statement sc, analytics_entity ae LEFT JOIN analytics_dataset ad ON ae.id = ad.entity_id
	WHERE
		ad.dataset = 'sanctions'
		AND sn.canonical_id = ae.id
		AND sc.canonical_id = ae.id
		AND sn.prop_type = 'name'
		AND sc.prop_type = 'country'
		AND ae.schema IN ('Person', 'Organization', 'Company', 'LegalEntity');
```