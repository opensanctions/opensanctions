ALTER TABLE review RENAME COLUMN model_version TO crawler_version;
ALTER TABLE review RENAME COLUMN orig_extraction_data TO original_extraction;
ALTER TABLE review ADD COLUMN origin VARCHAR(65535);
UPDATE review SET origin = 'gpt-4o' WHERE origin IS NULL AND dataset = 'us_ofac_enforcement_actions';
UPDATE review SET origin = 'gpt-4o' WHERE origin IS NULL AND dataset = 'sg_mas_enforcement_actions';
UPDATE review SET origin = 'gpt-4o' WHERE origin IS NULL AND dataset = 'us_fed_enforcements';
UPDATE review SET origin = 'gpt-4o' WHERE origin IS NULL AND dataset = 'us_al_med_exclusions';
