CREATE SEQUENCE IF NOT EXISTS data_catalog.bronze_backfill_control_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

CREATE TABLE IF NOT EXISTS data_catalog.bronze_backfill_control
(
    id integer NOT NULL DEFAULT nextval('data_catalog.bronze_backfill_control_id_seq'::regclass),
    tb_databases_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_schemas_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_tables_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_columns_id character varying(32) COLLATE pg_catalog."default",
    payload_limit integer NOT NULL DEFAULT 1000,
    target_timestamp timestamp without time zone,
    insert_timestamp timestamp without time zone,
    insert_done boolean NOT NULL DEFAULT false,
    update_timestamp timestamp without time zone,
    update_done boolean NOT NULL DEFAULT false,
    delete_timestamp timestamp without time zone,
    delete_done boolean NOT NULL DEFAULT false,
    created_at timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamp without time zone,
    CONSTRAINT bronze_backfill_control_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_backfill_database_schema_table
    ON data_catalog.bronze_backfill_control USING btree
    (tb_databases_id COLLATE pg_catalog."default" ASC NULLS LAST, tb_schemas_id COLLATE pg_catalog."default" ASC NULLS LAST, tb_tables_id COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default
    WHERE tb_columns_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_backfill_database_schema_table_column
    ON data_catalog.bronze_backfill_control USING btree
    (tb_databases_id COLLATE pg_catalog."default" ASC NULLS LAST, tb_schemas_id COLLATE pg_catalog."default" ASC NULLS LAST, tb_tables_id COLLATE pg_catalog."default" ASC NULLS LAST, tb_columns_id COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default
    WHERE tb_columns_id IS NOT NULL;