CREATE TABLE IF NOT EXISTS data_catalog.bronze_layer_control (
	tb_databases_id VARCHAR(32) NOT NULL,
	tb_schemas_id VARCHAR(32) NOT NULL,
	tb_tables_id VARCHAR(32) NOT NULL,
	target_timestamp TIMESTAMP WITHOUT TIME ZONE,
	insert_qty BIGINT,
	update_qty BIGINT,
	delete_qty BIGINT,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT clock_timestamp(),
	updated_at TIMESTAMP WITHOUT TIME ZONE,
	CONSTRAINT bronze_layer_control_pk PRIMARY KEY (tb_databases_id, tb_schemas_id, tb_tables_id)
);
COMMENT ON TABLE data_catalog.bronze_layer_control IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.tb_databases_id IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.tb_schemas_id IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.tb_tables_id IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.target_timestamp IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.insert_qty IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.update_qty IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.delete_qty IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.created_at IS '';
COMMENT ON COLUMN data_catalog.bronze_layer_control.updated_at IS '';
