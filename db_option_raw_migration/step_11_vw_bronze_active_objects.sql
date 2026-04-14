CREATE OR REPLACE VIEW data_catalog.vw_bronze_active_objects AS
SELECT	DISTINCT
		d.id AS database_id
		, d.name AS database_name
		, s.id AS schema_id
		, s.name AS schema_name
		, t.id AS table_id
		, t.name AS table_name
		, c.id AS column_id
		, c.name AS column_name
		, REPLACE(vw.bronze_schema_name, '"', '') AS bronze_schema
FROM data_catalog.tb_databases d
JOIN data_catalog.tb_schemas s ON s.tb_databases_id = d.id
JOIN data_catalog.tb_tables t ON t.tb_databases_id = s.tb_databases_id AND t.tb_schemas_id = s.id
JOIN data_catalog.tb_columns c ON c.tb_databases_id = t.tb_databases_id AND c.tb_schemas_id = t.tb_schemas_id AND c.tb_tables_id = t.id
JOIN data_catalog.vw_catalog vw ON vw.database_id = d.id AND vw.schema_id = s.id AND vw.table_id = t.id AND vw.column_id = c.id
WHERE d.tb_status_id = 5
AND d.active IS TRUE
AND s.tb_status_id = 5
AND s.active IS TRUE
AND t.tb_status_id = 5
AND t.active IS TRUE
AND c.tb_status_id = 5
AND c.active IS TRUE;
COMMENT ON VIEW data_catalog.vw_bronze_active_objects IS 'View que lista todos os objetos (databases, schemas, tables e columns) disponíveis na camada bronze e que estão com o tb_status_id = 5, recebendo CDC entre camada RAW e camada BRONZE';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.database_id IS 'ID do banco de dados. Referência data_catalog.tb_databases.id';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.database_name IS 'Nome do banco de dados. Referência data_catalog.tb_databases.name';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.schema_id IS 'ID do schema do banco de dados. Referência data_catalog.tb_schemas.id';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.schema_name IS 'Nome do schema do banco de dados. Referência data_catalog.tb_schemas.name';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.table_id IS 'ID da tabela do schema do banco de dados. Referência data_catalog.tb_tables.id';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.table_name IS 'Nome da tabela do schema do banco de dados. Referência data_catalog.tb_tables.name';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.column_id IS 'ID da coluna da tabela do schema do banco de dados. Referência data_catalog.tb_column.id';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.column_name IS 'Nome da coluna da tabela do schema do banco de dados. Referência data_catalog.tb_column.name';
COMMENT ON COLUMN data_catalog.vw_bronze_active_objects.bronze_schema IS 'Nome do schema do objeto completo criado no schema bronze';