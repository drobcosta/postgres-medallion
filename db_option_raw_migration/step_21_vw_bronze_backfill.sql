CREATE OR REPLACE VIEW data_catalog.vw_bronze_backfill
 AS
 SELECT DISTINCT vw.database_id,
    vw.database_name,
    vw.schema_id,
    vw.schema_name,
    vw.table_id,
    vw.table_name,
    bbc.insert_timestamp,
    bbc.insert_done,
    bbc.update_timestamp,
    bbc.update_done,
    bbc.delete_timestamp,
    bbc.delete_done
   FROM data_catalog.vw_catalog vw
     LEFT JOIN data_catalog.bronze_payload_control bpc ON bpc.tb_databases_id::text = vw.database_id::text AND bpc.tb_schemas_id::text = vw.schema_id::text AND bpc.tb_tables_id::text = vw.table_id::text
     LEFT JOIN data_catalog.bronze_backfill_control bbc ON bbc.tb_databases_id::text = vw.database_id::text AND bbc.tb_schemas_id::text = vw.schema_id::text AND bbc.tb_tables_id::text = vw.table_id::text AND bbc.tb_columns_id IS NULL
  WHERE vw.database_status_id = 5 AND vw.schema_status_id = 5 AND vw.table_status_id = 5 AND bpc.tb_tables_id IS NULL AND (bbc.id IS NULL OR bbc.insert_done IS FALSE OR bbc.update_done IS FALSE OR bbc.delete_done IS FALSE);