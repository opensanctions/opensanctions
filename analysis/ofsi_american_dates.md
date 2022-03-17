
```sql
SELECT
    sgb.entity_id AS ofsi_group_id, sgb.value as ofsi_birth_date, sgb.first_seen AS ofsi_first_seen, sus.dataset AS ofac_list, sus.entity_id AS ofac_id, sus.value AS ofac_birth_date
    FROM statement sgb, statement sus
    WHERE sgb.dataset = 'gb_hmt_sanctions' AND sus.dataset LIKE 'us_ofac%' AND sgb.canonical_id = sus.canonical_id AND sgb.prop = 'birthDate' AND sus.prop = 'birthDate' and sgb.value <> sus.value AND SUBSTR(sgb.value,  0, 5) = SUBSTR(sus.value, 0, 5)
    ORDER BY sgb.first_seen DESC; 
```