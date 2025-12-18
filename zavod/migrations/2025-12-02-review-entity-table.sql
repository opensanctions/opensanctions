CREATE TABLE review_entity (
    dataset VARCHAR(255) NOT NULL,
    review_key VARCHAR(255) NOT NULL,
    entity_id VARCHAR(255) NOT NULL,
    last_seen_version VARCHAR(255) NOT NULL
);

CREATE INDEX ix_review_entity_review_key ON review_entity (review_key);
CREATE INDEX ix_review_entity_last_seen_version ON review_entity (last_seen_version);
CREATE INDEX ix_review_entity_dataset ON review_entity (dataset);
CREATE UNIQUE INDEX ix_review_entity_unique_review_key_entity_id_dataset ON review_entity (review_key, entity_id, dataset);
