SELECT	DISTINCT
		sch.nspname AS schema_name
		, vw_catalog.schema_status_id
		, vw_catalog.schema_status_name
		, COALESCE(obj_description(sch.oid),vw_catalog.schema_description) AS schema_description
		, tbl.relname AS table_name
		, vw_catalog.table_status_id
		, vw_catalog.table_status_name
		, obj_description(tbl.oid) AS table_description
		, col.attname AS column_name
		, vw_catalog.column_status_id
		, vw_catalog.column_status_name
		, col_description(tbl.oid, col.attnum) AS column_description
FROM data_catalog.vw_catalog
JOIN pg_namespace sch ON sch.nspname = REPLACE(vw_catalog.bronze_schema_name,'"','')
JOIN pg_class tbl ON tbl.relname = vw_catalog.table_name
JOIN pg_attribute col ON col.attrelid = tbl.oid AND col.attname = vw_catalog.column_name
ORDER BY 1,3
