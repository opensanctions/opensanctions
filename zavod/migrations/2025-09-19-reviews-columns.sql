ALTER TABLE review RENAME COLUMN model_version TO crawler_version;
ALTER TABLE review RENAME COLUMN orig_extraction_data TO original_extraction;
ALTER TABLE review ADD COLUMN origin VARCHAR(65535);
UPDATE review SET origin = 'gpt-4o' WHERE origin IS NULL AND dataset in (
    'us_ofac_enforcement_actions',
    'us_cftc_enforcement_actions',
    'sg_mas_enforcement_actions',
    'us_fed_enforcements',
    'us_al_med_exclusions',
    'us_ofac_press_releases',
    'us_sec_litigation'
);
