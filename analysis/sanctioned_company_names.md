For Ian, SpendNetwork to do a JOIN on procurement providers:

```sql
SELECT ae.id AS id, na.value AS name
	FROM analytics_entity ae, analytics_dataset ad, statement na
	WHERE ae.id = ad.entity_id AND ad.dataset = 'sanctions'
		AND ae.schema in ('Organization', 'Company')
		AND na.canonical_id = ae.id AND na.prop_type = 'name';
```