CREATE SEQUENCE IF NOT EXISTS data_catalog.tb_raw_into_catalog_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

CREATE TABLE IF NOT EXISTS data_catalog.tb_raw_into_catalog
(
    id bigint NOT NULL DEFAULT nextval('data_catalog.tb_raw_into_catalog_id_seq'::regclass),
    object_type character varying COLLATE pg_catalog."default",
    object_qty bigint,
    payload_timestamp timestamp without time zone,
    CONSTRAINT tb_raw_into_catalog_pkey PRIMARY KEY (id)
);

CREATE OR REPLACE FUNCTION data_catalog.raw_into_catalog(
	)
    RETURNS TABLE(object_type character varying, object_qty bigint) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
	-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	-- Validação para evitar problemas na concorrência da chamada da função
	-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	IF NOT pg_try_advisory_lock(888888) THEN
	    RAISE NOTICE 'data_catalog.raw_into_catalog() já está em execução. Abortando.';
		RETURN QUERY
			SELECT	null AS object_type
					, null AS object_qty;
	    RETURN;
	END IF;
	
	RETURN QUERY
		WITH raw_data AS (
			SELECT	MD5(REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g')) AS database_id
					, REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g') AS database_name
					, MD5(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g')) AS schema_id
					, REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g') AS schema_name
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
			CROSS JOIN (
				SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
				FROM (
					SELECT tb_raw_patterns.pattern
					FROM data_catalog.tb_raw_patterns
					UNION ALL
					SELECT '_' AS pattern
				) patterns
			) tb_raw_patterns
			CROSS JOIN (
				SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
				FROM (
					SELECT trp.pattern FROM data_catalog.tb_raw_patterns trp
					UNION
					SELECT trdsep.pattern FROM data_catalog.tb_raw_databases_schemas_excluded_patterns trdsep
				) patterns
			) tb_raw_databases_schemas_excluded_patterns
			LEFT JOIN data_catalog.tb_columns tc
				ON tc.tb_databases_id = MD5(REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'))
				AND tc.tb_schemas_id = MD5(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g'))
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
			AND c.table_schema LIKE ANY (SELECT '%' || tb_raw_patterns.pattern FROM data_catalog.tb_raw_patterns)
			AND NULLIF(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g'),'') IS NOT NULL
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
		, tb_raw_into_catalog_add AS (
			INSERT INTO data_catalog.tb_raw_into_catalog (object_type,object_qty,payload_timestamp)
			SELECT objects.object::VARCHAR, objects.qty::BIGINT, CURRENT_TIMESTAMP
			FROM (
				SELECT 1 AS ordem, 'databases' AS object, COUNT(*) AS qty FROM tb_databases_add UNION ALL
				SELECT 2 AS ordem, 'schemas' AS object, COUNT(*) AS qty FROM tb_schemas_add UNION ALL
				SELECT 3 AS ordem, 'tables' AS object, COUNT(*) AS qty FROM tb_tables_add UNION ALL
				SELECT 4 AS ordem, 'columns' AS object, COUNT(*) AS qty FROM tb_columns_add
			) objects
			WHERE NOT (
				((SELECT COUNT(*) FROM tb_databases_add) = 0)
				AND ((SELECT COUNT(*) FROM tb_schemas_add) = 0)
				AND ((SELECT COUNT(*) FROM tb_tables_add) = 0)
				AND ((SELECT COUNT(*) FROM tb_columns_add) = 0)
			)
			ORDER BY objects.ordem
			RETURNING tb_raw_into_catalog.object_type, tb_raw_into_catalog.object_qty
		)
		SELECT objects.object_type::VARCHAR, objects.object_qty::BIGINT
		FROM tb_raw_into_catalog_add objects;
	RETURN;
END; 
$BODY$;

CREATE OR REPLACE FUNCTION data_catalog.inactivating_catalog_objects(
	)
    RETURNS TABLE(object_type character varying, object_qty bigint) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
	-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	-- Validação para evitar problemas na concorrência da chamada da função
	-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	IF NOT pg_try_advisory_lock(121212) THEN
	    RAISE NOTICE 'data_catalog.raw_into_catalog() já está em execução. Abortando.';
		RETURN QUERY
			SELECT	null AS object_type
					, null AS object_qty;
	    RETURN;
	END IF;
	
	RETURN QUERY
		WITH bronze_databases AS (
			SELECT DISTINCT d.id AS database_id, d.name AS database_name
			FROM data_catalog.tb_databases d
			JOIN data_catalog.tb_schemas s ON s.tb_databases_id = d.id
			JOIN pg_namespace sch
				ON sch.nspname = CONCAT(d.name,'_',s.name)
			LEFT JOIN LATERAL (
				SELECT	DISTINCT
						MD5(REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g')) AS database_id
						, REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g') AS database_name
				FROM information_schema.columns c
				JOIN pg_namespace sch
					ON sch.nspname = c.table_schema
				CROSS JOIN (
					SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
					FROM (
						SELECT tb_raw_patterns.pattern
						FROM data_catalog.tb_raw_patterns
						UNION ALL
						SELECT '_' AS pattern
					) patterns
				) tb_raw_patterns
				CROSS JOIN (
					SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
					FROM (
						SELECT trp.pattern FROM data_catalog.tb_raw_patterns trp
						UNION
						SELECT trdsep.pattern FROM data_catalog.tb_raw_databases_schemas_excluded_patterns trdsep
					) patterns
				) tb_raw_databases_schemas_excluded_patterns
				WHERE c.table_schema NOT IN ('information_schema','pg_catalog')
				AND c.table_schema LIKE ANY (SELECT '%' || tb_raw_patterns.pattern FROM data_catalog.tb_raw_patterns)
				AND NULLIF(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g'),'') IS NOT NULL
			) raw ON raw.database_id = d.id
			WHERE d.active IS TRUE
			AND raw.database_id IS NULL
		)
		, disable_databases AS (
			UPDATE data_catalog.tb_databases d SET active = FALSE, updated_at = clock_timestamp()
			FROM bronze_databases bd
			WHERE bd.database_id = d.id
			RETURNING d.id
		)
		, bronze_schemas AS (
			SELECT	DISTINCT 
					d.id AS database_id
					, d.name AS database_name
					, s.id AS schema_id
					, s.name AS schema_name
					, CONCAT(d.name,'_',s.name) AS bronze_schema_name
			FROM data_catalog.tb_databases d
			JOIN data_catalog.tb_schemas s ON s.tb_databases_id = d.id
			JOIN pg_namespace sch
				ON sch.nspname = CONCAT(d.name,'_',s.name)
			LEFT JOIN LATERAL (
				SELECT	raw.database_id
						, raw.database_name
						, raw.schema_id
						, raw.schema_name
						, CONCAT(raw.database_name,'_',raw.schema_name) AS bronze_schema_name
				FROM (
					SELECT	DISTINCT
							MD5(REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g')) AS database_id
							, REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g') AS database_name
							, MD5(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g')) AS schema_id
							, REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g') AS schema_name
					FROM information_schema.columns c
					JOIN pg_namespace sch
						ON sch.nspname = c.table_schema
					CROSS JOIN (
						SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
						FROM (
							SELECT tb_raw_patterns.pattern
							FROM data_catalog.tb_raw_patterns
							UNION ALL
							SELECT '_' AS pattern
						) patterns
					) tb_raw_patterns
					CROSS JOIN (
						SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
						FROM (
							SELECT trp.pattern FROM data_catalog.tb_raw_patterns trp
							UNION
							SELECT trdsep.pattern FROM data_catalog.tb_raw_databases_schemas_excluded_patterns trdsep
						) patterns
					) tb_raw_databases_schemas_excluded_patterns
					WHERE c.table_schema NOT IN ('information_schema','pg_catalog')
					AND c.table_schema LIKE ANY (SELECT '%' || tb_raw_patterns.pattern FROM data_catalog.tb_raw_patterns)
					AND NULLIF(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g'),'') IS NOT NULL
				) raw
			) raw ON raw.bronze_schema_name = CONCAT(d.name,'_',s.name)
			WHERE d.active IS TRUE
			AND s.active IS TRUE
			AND raw.bronze_schema_name IS NULL
		)
		, disable_schemas AS (
			UPDATE data_catalog.tb_schemas s SET active = FALSE, updated_at = clock_timestamp()
			FROM bronze_schemas bs
			WHERE bs.database_id = s.tb_databases_id
			AND bs.schema_id = s.id
			RETURNING s.id
		)
		, bronze_tables AS (
			SELECT	DISTINCT 
					d.id AS database_id
					, d.name AS database_name
					, s.id AS schema_id
					, s.name AS schema_name
					, t.id AS table_id
					, t.name AS table_name
					, CONCAT(d.name,'_',s.name,'.',t.name) AS bronze_table_path
			FROM data_catalog.tb_databases d
			JOIN data_catalog.tb_schemas s ON s.tb_databases_id = d.id
			JOIN data_catalog.tb_tables t ON t.tb_databases_id = d.id AND t.tb_schemas_id = s.id
			JOIN pg_namespace sch
				ON sch.nspname = CONCAT(d.name,'_',s.name)
			JOIN pg_class cl
				ON cl.relnamespace = sch.oid
				AND cl.relname = t.name
				AND cl.relkind = 'r'
			LEFT JOIN LATERAL (
				SELECT	raw.database_id
						, raw.database_name
						, raw.schema_id
						, raw.schema_name
						, raw.table_id
						, raw.table_name
						, CONCAT(raw.database_name,'_',raw.schema_name,'.',raw.table_name) AS bronze_table_path
				FROM (
					SELECT	DISTINCT
							MD5(REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g')) AS database_id
							, REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g') AS database_name
							, MD5(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g')) AS schema_id
							, REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g') AS schema_name
							, MD5(c.table_name) AS table_id
							, c.table_name AS table_name
					FROM information_schema.columns c
					JOIN pg_namespace sch
						ON sch.nspname = c.table_schema
					JOIN pg_class cl
						ON cl.relnamespace = sch.oid
						AND cl.relname = c.table_name
						AND cl.relkind = 'r'
					CROSS JOIN (
						SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
						FROM (
							SELECT tb_raw_patterns.pattern
							FROM data_catalog.tb_raw_patterns
							UNION ALL
							SELECT '_' AS pattern
						) patterns
					) tb_raw_patterns
					CROSS JOIN (
						SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
						FROM (
							SELECT trp.pattern FROM data_catalog.tb_raw_patterns trp
							UNION
							SELECT trdsep.pattern FROM data_catalog.tb_raw_databases_schemas_excluded_patterns trdsep
						) patterns
					) tb_raw_databases_schemas_excluded_patterns
					WHERE c.table_schema NOT IN ('information_schema','pg_catalog')
					AND c.table_schema LIKE ANY (SELECT '%' || tb_raw_patterns.pattern FROM data_catalog.tb_raw_patterns)
					AND NULLIF(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g'),'') IS NOT NULL
				) raw
			) raw ON raw.bronze_table_path = CONCAT(d.name,'_',s.name,'.',t.name)
			WHERE d.active IS TRUE
			AND s.active IS TRUE
			AND t.active IS TRUE
			AND raw.bronze_table_path IS NULL
		)
		, disable_tables AS (
			UPDATE data_catalog.tb_tables t SET active = FALSE, updated_at = clock_timestamp()
			FROM bronze_tables bt
			WHERE bt.database_id = t.tb_databases_id
			AND bt.schema_id = t.tb_schemas_id
			AND bt.table_id = t.id
			RETURNING t.id
		)
		, bronze_columns AS (
			SELECT	DISTINCT
					d.id AS database_id
					, d.name AS database_name
					, s.id AS schema_id
					, s.name AS schema_name
					, t.id AS table_id
					, t.name AS table_name
					, c.id AS column_id
					, c.name AS column_name
			FROM data_catalog.tb_databases d
			JOIN data_catalog.tb_schemas s ON s.tb_databases_id = d.id
			JOIN data_catalog.tb_tables t ON t.tb_databases_id = d.id AND t.tb_schemas_id = s.id
			JOIN data_catalog.tb_columns c ON c.tb_databases_id = d.id AND c.tb_schemas_id = s.id AND c.tb_tables_id = t.id
			JOIN pg_namespace sch
				ON sch.nspname = CONCAT(d.name,'_',s.name)
			JOIN pg_class cl
				ON cl.relnamespace = sch.oid
				AND cl.relname = t.name
				AND cl.relkind = 'r'
			LEFT JOIN LATERAL (
				SELECT	raw.database_id
						, raw.database_name
						, raw.schema_id
						, raw.schema_name
						, raw.table_id
						, raw.table_name
						, raw.column_id
						, raw.column_name
						, CONCAT(raw.database_name,'_',raw.schema_name,'.',raw.table_name,'.',raw.column_name) AS bronze_column_path
				FROM (
					SELECT	DISTINCT
							MD5(REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g')) AS database_id
							, REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g') AS database_name
							, MD5(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g')) AS schema_id
							, REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g') AS schema_name
							, MD5(c.table_name) AS table_id
							, c.table_name AS table_name
							, MD5(c.column_name) AS column_id
							, c.column_name AS column_name
					FROM information_schema.columns c
					JOIN pg_namespace sch
						ON sch.nspname = c.table_schema
					JOIN pg_class cl
						ON cl.relnamespace = sch.oid
						AND cl.relname = c.table_name
						AND cl.relkind = 'r'
					CROSS JOIN (
						SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
						FROM (
							SELECT tb_raw_patterns.pattern
							FROM data_catalog.tb_raw_patterns
							UNION ALL
							SELECT '_' AS pattern
						) patterns
					) tb_raw_patterns
					CROSS JOIN (
						SELECT CONCAT('(', string_agg(patterns.pattern,'|'), ')') AS pattern
						FROM (
							SELECT trp.pattern FROM data_catalog.tb_raw_patterns trp
							UNION
							SELECT trdsep.pattern FROM data_catalog.tb_raw_databases_schemas_excluded_patterns trdsep
						) patterns
					) tb_raw_databases_schemas_excluded_patterns
					WHERE c.table_schema NOT IN ('information_schema','pg_catalog')
					AND c.table_schema LIKE ANY (SELECT '%' || tb_raw_patterns.pattern FROM data_catalog.tb_raw_patterns)
					AND NULLIF(REGEXP_REPLACE((REPLACE(c.table_schema,REGEXP_REPLACE(c.table_schema,tb_raw_databases_schemas_excluded_patterns.pattern, '', 'g'),'')),tb_raw_patterns.pattern, '', 'g'),'') IS NOT NULL
				) raw
			) raw ON raw.bronze_column_path = CONCAT(d.name,'_',s.name,'.',t.name,'.',c.name)
			WHERE d.active IS TRUE
			AND s.active IS TRUE
			AND t.active IS TRUE
			AND c.active IS TRUE
			AND raw.bronze_column_path IS NULL
		)
		, disable_columns AS (
			UPDATE data_catalog.tb_columns c SET active = FALSE, updated_at = clock_timestamp()
			FROM bronze_columns bc
			WHERE bc.database_id = c.tb_databases_id
			AND bc.schema_id = c.tb_schemas_id
			AND bc.table_id = c.tb_tables_id
			AND bc.column_id = c.id
			RETURNING c.id
		)
		, tb_raw_into_catalog_add AS (
			INSERT INTO data_catalog.tb_raw_into_catalog (object_type,object_qty,payload_timestamp)
			SELECT objects.object::VARCHAR, objects.qty::BIGINT, CURRENT_TIMESTAMP
			FROM (
				SELECT 1 AS ordem, 'databases' AS object, -COUNT(*) AS qty FROM disable_databases UNION ALL
				SELECT 2 AS ordem, 'schemas' AS object, -COUNT(*) AS qty FROM disable_schemas UNION ALL
				SELECT 3 AS ordem, 'tables' AS object, -COUNT(*) AS qty FROM disable_tables UNION ALL
				SELECT 4 AS ordem, 'columns' AS object, -COUNT(*) AS qty FROM disable_columns
			) objects
			WHERE NOT (
				((SELECT COUNT(*) FROM disable_databases) = 0)
				AND ((SELECT COUNT(*) FROM disable_schemas) = 0)
				AND ((SELECT COUNT(*) FROM disable_tables) = 0)
				AND ((SELECT COUNT(*) FROM disable_columns) = 0)
			)
			ORDER BY objects.ordem
			RETURNING tb_raw_into_catalog.object_type, tb_raw_into_catalog.object_qty
		)
		SELECT objects.object_type::VARCHAR, objects.object_qty::BIGINT
		FROM tb_raw_into_catalog_add objects;
	RETURN;
END; 
$BODY$;
