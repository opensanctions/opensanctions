```sql
SELECT ae.id, ae.caption,
		STRING_AGG(DISTINCT ac.country, '; ') AS countries,
		STRING_AGG(DISTINCT sn.value, '; ') AS names,
		COUNT(DISTINCT ssa.entity_id) AS sanctions
	FROM analytics_entity ae, statement ssa, statement ssl, analytics_country ac, statement sn
	WHERE
		ae.id = ssl.canonical_id
		AND sn.canonical_id = ae.id
		AND sn.prop_type = 'name'
		AND ac.entity_id = ae.id
		AND ssl.entity_id = ssa.value
		AND ssa.schema = 'Sanction'
		AND ae.schema = 'Person'
		AND ac.country IN ('ru', 'by', 'suhh')
	GROUP BY ae.id, ae.caption
	ORDER BY COUNT(DISTINCT ssa.entity_id) DESC;  
```