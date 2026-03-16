CREATE OR REPLACE FUNCTION data_catalog.raw_into_catalog()
RETURNS TABLE (
	object_type VARCHAR,
	object_qty BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
BEGIN
	RETURN QUERY
		WITH raw_data AS (
			SELECT	MD5(REGEXP_REPLACE(c.table_schema,'(_public|_raw)', '', 'g')) AS database_id
					, REGEXP_REPLACE(c.table_schema,'(_public|_raw)', '', 'g') AS database_name
					, MD5(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,'(_public|_raw)', '', 'g'),'')),'(raw|_)', '', 'g')) AS schema_id
					, REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,'(_public|_raw)', '', 'g'),'')),'(raw|_)', '', 'g') AS schema_name
					, MD5(c.table_name) AS table_id
					, c.table_name AS table_name
					, cl.relkind AS table_type
					, MD5(c.column_name) AS column_id
					, c.column_name AS column_name
					, CASE WHEN (pk_columns.column_name IS NOT NULL) THEN true ELSE false END AS column_pk
			FROM information_schema.columns c
			JOIN pg_namespace sch
				ON sch.nspname = c.table_schema
			JOIN pg_class cl
				ON cl.relnamespace = sch.oid
				AND cl.relname = c.table_name
				AND cl.relkind = 'r'
			LEFT JOIN data_catalog.tb_columns tc
				ON tc.tb_databases_id = MD5(REGEXP_REPLACE(c.table_schema,'(_public|_raw)', '', 'g'))
				AND tc.tb_schemas_id = MD5(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,'(_public|_raw)', '', 'g'),'')),'(raw|_)', '', 'g'))
				AND tc.tb_tables_id = MD5(c.table_name)
				AND tc.id = MD5(c.column_name)
			LEFT JOIN LATERAL (
				SELECT	istc.table_schema
						, istc.table_name
						, iskcu.column_name
				FROM information_schema.table_constraints istc
				JOIN information_schema.key_column_usage iskcu
					ON iskcu.table_schema = istc.table_schema
					AND iskcu.table_name = istc.table_name
					AND iskcu.constraint_name = istc.constraint_name
				WHERE istc.table_schema = c.table_schema
				AND istc.table_name = c.table_name
				AND istc.constraint_type = 'PRIMARY KEY'
			) pk_columns
				ON pk_columns.table_schema = c.table_schema
				AND pk_columns.table_name = c.table_name
				AND pk_columns.column_name = c.column_name
			WHERE c.table_schema NOT IN ('information_schema','pg_catalog')
			AND c.table_schema LIKE '%_raw'
			AND tc.id IS NULL
			ORDER BY 2,4,6
		)
		, tb_databases_add AS (
			INSERT INTO data_catalog.tb_databases (id, "name", tb_status_id, active, created_at)
			SELECT	DISTINCT
					rd.database_id
					, rd.database_name
					, 1 AS tb_status_id
					, true AS active
					, clock_timestamp() AS created_at
			FROM raw_data rd
			ON CONFLICT (id) DO NOTHING
			RETURNING id
		)
		, tb_schemas_add AS (
			INSERT INTO data_catalog.tb_schemas (id, tb_databases_id, tb_status_id, "name", active, created_at)
			SELECT	DISTINCT
					rd.schema_id
					, COALESCE(tda.id,rd.database_id) AS tb_databases_id
					, 1 AS tb_status_id
					, rd.schema_name
					, true AS active
					, clock_timestamp() AS created_at
			FROM raw_data rd
			LEFT JOIN tb_databases_add tda
				ON tda.id = rd.database_id
			ON CONFLICT (id, tb_databases_id) DO NOTHING
			RETURNING id, tb_databases_id
		)
		, tb_tables_add AS (
			INSERT INTO data_catalog.tb_tables (id, tb_databases_id, tb_schemas_id, tb_status_id, "name", active, created_at)
			SELECT	DISTINCT
					rd.table_id
					, COALESCE(tsa.tb_databases_id, rd.database_id) AS tb_databases_id
					, COALESCE(tsa.id, rd.schema_id) AS tb_schemas_id
					, 1 AS tb_status_id
					, rd.table_name
					, true AS active
					, clock_timestamp() AS created_at
			FROM raw_data rd
			LEFT JOIN tb_databases_add tda
				ON tda.id = rd.database_id
			LEFT JOIN tb_schemas_add tsa
				ON tsa.id = rd.schema_id
				AND tsa.tb_databases_id = rd.database_id
			ON CONFLICT (id, tb_databases_id, tb_schemas_id) DO NOTHING
			RETURNING id, tb_databases_id, tb_schemas_id
		)
		, tb_columns_add AS (
			INSERT INTO data_catalog.tb_columns (id, tb_databases_id, tb_schemas_id, tb_tables_id, tb_status_id, "name", is_pk, active, created_at)
			SELECT	DISTINCT
					rd.column_id
					, COALESCE(tta.tb_databases_id,rd.database_id) AS tb_databases_id
					, COALESCE(tta.tb_schemas_id,rd.schema_id) AS tb_schemas_id
					, COALESCE(tta.id,rd.table_id) AS tb_tables_id
					, 1 AS tb_status_id
					, rd.column_name
					, rd.column_pk
					, true AS active
					, clock_timestamp() AS created_at
			FROM raw_data rd
			LEFT JOIN tb_databases_add tda
				ON tda.id = rd.database_id
			LEFT JOIN tb_schemas_add tsa
				ON tsa.id = rd.schema_id
				AND tsa.tb_databases_id = rd.database_id
			LEFT JOIN tb_tables_add tta
				ON tta.id = rd.table_id
				AND tta.tb_databases_id = rd.database_id
				AND tta.tb_schemas_id = rd.schema_id
			ON CONFLICT (id, tb_databases_id, tb_schemas_id, tb_tables_id) DO NOTHING
			RETURNING tb_databases_id, tb_schemas_id, tb_tables_id, id
		)
		SELECT objects.object::VARCHAR, objects.qty::BIGINT
		FROM (
			SELECT 1 AS ordem, 'databases' AS object, COUNT(*) AS qty FROM tb_databases_add UNION ALL
			SELECT 2 AS ordem, 'schemas' AS object, COUNT(*) AS qty FROM tb_schemas_add UNION ALL
			SELECT 3 AS ordem, 'tables' AS object, COUNT(*) AS qty FROM tb_tables_add UNION ALL
			SELECT 4 AS ordem, 'columns' AS object, COUNT(*) AS qty FROM tb_columns_add
		) objects
		ORDER BY objects.ordem;
	RETURN;
END; $BODY$;
