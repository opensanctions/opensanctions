ALTER TABLE review RENAME COLUMN model_version TO crawler_version;
ALTER TABLE review RENAME COLUMN orig_extraction_data TO original_extraction;
ALTER TABLE review ADD COLUMN origin VARCHAR(65535);
