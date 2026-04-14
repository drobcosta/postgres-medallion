CREATE TABLE IF NOT EXISTS data_catalog.bronze_payload_control
(
    tb_databases_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_schemas_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_tables_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    insert_timestamp timestamp without time zone,
    insert_qty bigint,
    update_timestamp timestamp without time zone,
    update_qty bigint,
    delete_timestamp timestamp without time zone,
    delete_qty bigint,
    created_at timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamp without time zone,
    CONSTRAINT bronze_layer_control_pk PRIMARY KEY (tb_databases_id, tb_schemas_id, tb_tables_id)
);