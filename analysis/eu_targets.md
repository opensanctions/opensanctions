
For Sarunas:

```sql
SELECT ae.id AS id, ae.schema AS type, ac.country AS country, ae.caption AS main_name, sn.value AS name,
	(SELECT string_agg(x, '; ') FROM jsonb_array_elements_text(ae.properties->'birthDate') AS x) AS birth_dates
	FROM analytics_entity ae, analytics_country ac, analytics_dataset ad, statement sn WHERE ae.id = ac.entity_id AND ac.country IN ('ru', 'by') AND ad.entity_id = ae.id AND ad.dataset = 'eu_fsf' AND ae.schema IN ('Person', 'LegalEntity', 'Organization', 'Company') AND sn.canonical_id = ae.id AND sn.prop_type = 'name';
```