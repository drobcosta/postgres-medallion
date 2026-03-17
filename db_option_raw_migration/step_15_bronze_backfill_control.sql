CREATE TABLE IF NOT EXISTS data_catalog.bronze_backfill_control (
	id SERIAL PRIMARY KEY,
	tb_databases_id VARCHAR(32) NOT NULL,
	tb_schemas_id VARCHAR(32) NOT NULL,
	tb_tables_id VARCHAR(32) NOT NULL,
	tb_columns_id VARCHAR(32) NULL,
	payload_limit INTEGER NOT NULL DEFAULT 1000,
	target_timestamp TIMESTAMP WITHOUT TIME ZONE,
	insert_timestamp TIMESTAMP WITHOUT TIME ZONE,
	insert_done BOOLEAN NOT NULL DEFAULT FALSE,
	update_timestamp TIMESTAMP WITHOUT TIME ZONE,
	update_done BOOLEAN NOT NULL DEFAULT FALSE,
	delete_timestamp TIMESTAMP WITHOUT TIME ZONE,
	delete_done BOOLEAN NOT NULL DEFAULT FALSE,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT clock_timestamp(),
	updated_at TIMESTAMP WITHOUT TIME ZONE
);
CREATE UNIQUE INDEX uq_backfill_database_schema_table
ON data_catalog.bronze_backfill_control (
    tb_databases_id, tb_schemas_id, tb_tables_id
) WHERE tb_columns_id IS NULL;
CREATE UNIQUE INDEX uq_backfill_database_schema_table_column
ON data_catalog.bronze_backfill_control (
    tb_databases_id, tb_schemas_id, tb_tables_id, tb_columns_id
)
WHERE tb_columns_id IS NOT NULL;
COMMENT ON TABLE data_catalog.bronze_backfill_control IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.id IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.tb_databases_id IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.tb_schemas_id IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.tb_tables_id IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.tb_columns_id IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.payload_limit IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.target_timestamp IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.insert_timestamp IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.insert_done IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.update_timestamp IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.update_done IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.delete_timestamp IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.delete_done IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.created_at IS '';
COMMENT ON COLUMN data_catalog.bronze_backfill_control.updated_at IS '';
