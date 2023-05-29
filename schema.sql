-- 2022-01-06: original 

CREATE TABLE alembic_version (
    version_num character varying(32) NOT NULL
);

CREATE TABLE analytics_country (
    entity_id character varying(255) NOT NULL,
    country character varying(255) NOT NULL
);

CREATE TABLE analytics_dataset (
    entity_id character varying(255) NOT NULL,
    dataset character varying(255) NOT NULL
);

CREATE TABLE analytics_entity (
    id character varying(255) NOT NULL,
    schema character varying(255) NOT NULL,
    caption character varying(65535) NOT NULL,
    target boolean,
    properties jsonb,
    first_seen timestamp without time zone,
    last_seen timestamp without time zone
);

CREATE TABLE cache (
    url character varying NOT NULL,
    text character varying,
    dataset character varying NOT NULL,
    "timestamp" timestamp without time zone
);

CREATE TABLE canonical (
    entity_id character varying(255) NOT NULL,
    canonical_id character varying(255)
);

CREATE TABLE issue (
    id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    level character varying(255) NOT NULL,
    module character varying(255),
    dataset character varying(255) NOT NULL,
    message character varying(65535),
    entity_id character varying(255),
    entity_schema character varying(255),
    data json NOT NULL
);

CREATE SEQUENCE issue_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE issue_id_seq OWNED BY issue.id;

CREATE TABLE resource (
    path character varying(255) NOT NULL,
    dataset character varying(255) NOT NULL,
    checksum character varying(255) NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    mime_type character varying(255),
    size integer,
    title character varying(65535)
);

CREATE TABLE statement (
    id character varying(255) NOT NULL,
    entity_id character varying(255) NOT NULL,
    canonical_id character varying(255),
    prop character varying(255) NOT NULL,
    prop_type character varying(255) NOT NULL,
    schema character varying(255) NOT NULL,
    value character varying(65535) NOT NULL,
    dataset character varying(255),
    target boolean NOT NULL,
    "unique" boolean NOT NULL,
    first_seen timestamp without time zone NOT NULL,
    last_seen timestamp without time zone
);

ALTER TABLE ONLY issue ALTER COLUMN id SET DEFAULT nextval('issue_id_seq'::regclass);
ALTER TABLE ONLY alembic_version ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);
ALTER TABLE ONLY issue ADD CONSTRAINT issue_pkey PRIMARY KEY (id);
ALTER TABLE ONLY resource ADD CONSTRAINT resource_pkey PRIMARY KEY (path, dataset);
ALTER TABLE ONLY statement ADD CONSTRAINT statement_pkey PRIMARY KEY (id);
CREATE INDEX ix_analytics_country_country ON analytics_country USING btree (country);
CREATE INDEX ix_analytics_country_entity_id ON analytics_country USING btree (entity_id);
CREATE INDEX ix_analytics_dataset_dataset ON analytics_dataset USING btree (dataset);
CREATE INDEX ix_analytics_dataset_entity_id ON analytics_dataset USING btree (entity_id);
CREATE INDEX ix_analytics_entity_id ON analytics_entity USING btree (id);
CREATE INDEX ix_cache_timestamp ON cache USING btree ("timestamp");
CREATE UNIQUE INDEX ix_cache_url ON cache USING btree (url);
CREATE INDEX ix_canonical_entity_id ON canonical USING btree (entity_id);
CREATE INDEX ix_issue_dataset ON issue USING btree (dataset);
CREATE INDEX ix_issue_entity_id ON issue USING btree (entity_id);
CREATE INDEX ix_resource_dataset ON resource USING btree (dataset);

CREATE INDEX ix_statement_canonical_id ON statement USING btree (canonical_id);
CREATE INDEX ix_statement_dataset ON statement USING btree (dataset);
CREATE INDEX ix_statement_entity_id ON statement USING btree (entity_id);
CREATE INDEX ix_statement_last_seen ON statement USING btree (last_seen);

-- 2022-04-13: remove alembic version, remove unique column on statement.

DROP TABLE alembic_version;
ALTER TABLE "statement" DROP COLUMN "unique";

--- 2022-05-18: rename url to key in cache table.

ALTER TABLE "cache" RENAME COLUMN "url" TO "key";

--- 2022-05-23: add external column for pre-prod facts.

ALTER TABLE "statement" ADD COLUMN "external" boolean DEFAULT false;

--- 2022-10-31: add extra properties on statements

ALTER TABLE "statement" ADD COLUMN "lang" character varying(255) DEFAULT NULL;
ALTER TABLE "statement" ADD COLUMN "original_value" character varying(65535) DEFAULT NULL;

--- 2023-04-11: swap out last seen index

DROP INDEX ix_statement_last_seen;
CREATE INDEX ix_statement_last_seen_dataset ON statement (last_seen, dataset);
CREATE INDEX ix_statement_type_external ON statement (prop_type, external);

--- 2023-05-09: add category column to resources
ALTER TABLE "resource" ADD COLUMN "category" character varying(255) DEFAULT NULL;

--- revert 2023-04-11: indexes

DROP INDEX ix_statement_last_seen_dataset;
DROP INDEX ix_statement_type_external;