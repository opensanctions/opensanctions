```sql
SELECT secprop.value, secprop.dataset AS isin_source, ae.id, ae.schema, ae.caption, ARRAY_AGG(ad.dataset)
	FROM statement secprop, statement secissuer, statement entid, analytics_entity ae, analytics_dataset ad
	WHERE secprop.prop = 'isin'
	AND secprop.external = false
	AND secprop.schema = 'Security'
	AND secprop.canonical_id = secissuer.canonical_id
	AND secissuer.prop = 'issuer'
	AND entid.entity_id = secissuer.value
	AND entid.prop = 'id'
	AND ae.id = entid.canonical_id
	AND ad.entity_id = ae.id
	GROUP BY secprop.value, secprop.dataset, ae.id, ae.schema, ae.caption
	HAVING NOT 'us_occ_enfact' = ANY(ARRAY_AGG(ad.dataset));
```