
```sql
SELECT data::jsonb->>'value', MAX(data::jsonb->>'prop'), COUNT(*) FROM issue WHERE message LIKE 'Rejected%' GROUP BY data::jsonb->>'value' ORDER BY COUNT(*) DESC;
```