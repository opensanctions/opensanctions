ALTER TABLE review RENAME COLUMN model_version TO crawler_version;
ALTER TABLE review ADD COLUMN extraction_checksum VARCHAR(255);
