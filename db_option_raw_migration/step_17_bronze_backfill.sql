CREATE OR REPLACE FUNCTION data_catalog.bronze_backfill_inserts(p_databases_id VARCHAR DEFAULT NULL, p_schemas_id VARCHAR DEFAULT NULL, p_tables_id VARCHAR DEFAULT NULL, p_columns_id VARCHAR DEFAULT NULL)
RETURNS SETOF data_catalog.bronze_backfill_control
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
-- VARIÁVEIS AUXILIARES E DE CONTROLE
DECLARE v_record RECORD;
DECLARE v_cmd VARCHAR;
-- VARIAVEIS GLOBAIS
DECLARE v_payload_limit INTEGER;
DECLARE v_bronze_columns VARCHAR;
DECLARE v_hstlog_new_columns VARCHAR;
DECLARE v_hstlog_old_columns VARCHAR;
DECLARE v_bronze_columns_pk_name VARCHAR;
DECLARE v_bronze_columns_pk VARCHAR;
DECLARE v_target_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp();
DECLARE v_qty INTEGER DEFAULT 0;
-- VARIÁVEIS DE OPERAÇÕES DE INSERT
DECLARE v_insert_timestamp TIMESTAMP WITHOUT TIME ZONE;
BEGIN
	-- Forçando fullcharge em determinada tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	IF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NULL THEN

		FOR v_record IN
			SELECT	DISTINCT
					vw.database_id
					, vw.database_name
					, vw.schema_id
					, vw.schema_name
					, vw.table_id
					, vw.table_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"') AS bronze_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"') AS raw_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"') AS hstlog_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"."',vw.table_name,'"') AS bronze_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"."',vw.table_name,'"') AS raw_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog"') AS hstlog_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_insert"') AS hstlog_insert_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_update"') AS hstlog_update_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_delete"') AS hstlog_delete_path
					, bbc.id AS bronze_layer_control_id
					, bbc.payload_limit
					, bbc.target_timestamp
					, bbc.insert_timestamp
					, bbc.insert_done
					, bbc.update_timestamp
					, bbc.update_done
					, bbc.delete_timestamp
					, bbc.delete_done
			FROM data_catalog.vw_catalog vw
			LEFT JOIN data_catalog.bronze_layer_control blc
				ON blc.tb_databases_id = vw.database_id
				AND blc.tb_schemas_id = vw.schema_id
				AND blc.tb_tables_id = vw.table_id
			LEFT JOIN data_catalog.bronze_backfill_control bbc
				ON bbc.tb_databases_id = vw.database_id
				AND bbc.tb_schemas_id = vw.schema_id
				AND bbc.tb_tables_id = vw.table_id
				AND bbc.tb_columns_id IS NULL
			WHERE vw.database_status_id = 5
			AND vw.schema_status_id = 5
			AND vw.table_status_id = 5
			AND blc.tb_tables_id IS NULL
			AND (
				bbc.id IS NULL
				OR bbc.insert_done IS FALSE
			)
			AND vw.database_id = p_databases_id
			AND vw.schema_id = p_schemas_id
			AND vw.table_id = p_tables_id
		LOOP
			-- Setando a quantidade limite de dados dentro do chunk do fullcharge
			-- Caso não esteja previamente definido, precisaremos atualizar as estatísticas da tabela para pegar uma quantidade de 25% de n_live_tup
			IF v_record.payload_limit IS NULL THEN
				-- Atualizando as estatísticas para pegar 25% da tabela
				v_cmd := $cmd$ ANALYZE $cmd$ || v_record.raw_path;
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
				END IF;

				SELECT ROUND(n_live_tup * 0.25)::INTEGER 
				FROM pg_stat_user_tables 
				WHERE schemaname = REPLACE(v_record.raw_schema_name,'"','')
				AND relname = v_record.table_name
				INTO v_payload_limit;
			ELSE
				v_payload_limit := v_record.payload_limit;
			END IF;

			-- Adicionando à variável v_target_timestamp a data limite para o fullcharge
			-- Caso o valor retorne null da query em loop, deixaremos o default definido na variável
			IF v_record.target_timestamp IS NOT NULL THEN
				v_target_timestamp := v_record.target_timestamp;
			END IF;

			-- Coletando os nomes das colunas para uma operação (INSERT, UPDATE, DELETE) concisos e ordenados
			SELECT 	string_agg(CONCAT('"', column_name, '"'),', ') AS bronze_columns
					, string_agg(CONCAT('("',v_record.table_name,'_new"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_new_columns
					, string_agg(CONCAT('("',v_record.table_name,'_old"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_old_columns
			FROM (
				SELECT column_name, data_type
				FROM information_schema.columns
				WHERE table_schema = REPLACE(v_record.bronze_schema_name,'"','')
				AND table_name = v_record.table_name
				ORDER BY ordinal_position
			) columns
			INTO v_bronze_columns, v_hstlog_new_columns, v_hstlog_old_columns;

			-- Coletando os nomes das colunas que são PK para UPSERT
			SELECT pk_name, CONCAT('(',string_agg(CONCAT('"',column_name,'"'),', '),')') AS bronze_columns_pk
			FROM (
				SELECT tc.constraint_name AS pk_name, c.column_name, c.data_type
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
					ON kcu.table_schema = tc.table_schema
					AND kcu.table_name = tc.table_name
					AND kcu.constraint_name = tc.constraint_name
				JOIN information_schema.columns c
					ON c.table_schema = tc.table_schema
					AND c.table_name = tc.table_name
					AND c.column_name = kcu.column_name
				WHERE tc.table_schema = REPLACE(v_record.bronze_schema_name,'"','')
				AND tc.table_name = v_record.table_name
				AND tc.constraint_type = 'PRIMARY KEY'
			) columns_pk
			GROUP BY pk_name
			INTO v_bronze_columns_pk_name, v_bronze_columns_pk;
			
			-- ======================================================================================================================================================
			-- ======================================================================================================================================================
			-- ======================================================================================================================================================
			IF v_record.insert_done IS NULL OR v_record.insert_done IS FALSE THEN

				-- Coletando a data de referência para o chunk
				-- Caso esteja nula, a data utilizada será a primeira data encontrada na tabela histórica
				IF v_record.insert_timestamp IS NULL THEN
					v_cmd := $cmd$ SELECT MIN(executed_at) FROM $cmd$ || v_record.hstlog_insert_path;
					IF v_cmd IS NOT NULL THEN
						EXECUTE v_cmd INTO v_insert_timestamp;
					END IF;
				ELSE
					v_insert_timestamp := v_record.insert_timestamp;
				END IF;

				-- Carga do chunk fullcharge da tabela
				v_cmd := $cmd$
					WITH datasource AS (
						SELECT	$cmd$ || v_hstlog_new_columns || $cmd$
								, executed_at
						FROM $cmd$ || v_record.hstlog_insert_path || $cmd$
						WHERE executed_at >= '$cmd$ || v_insert_timestamp || $cmd$'
						AND executed_at < '$cmd$ || v_target_timestamp || $cmd$'
						ORDER BY executed_at ASC
						LIMIT $cmd$ || v_payload_limit || $cmd$
					), backfill_registry AS (
						SELECT MAX(ds.executed_at) AS insert_timestamp
						FROM datasource ds
					), bronze_registry AS (
						INSERT INTO $cmd$ || v_record.bronze_path || $cmd$ ($cmd$ || v_bronze_columns || $cmd$)
						SELECT $cmd$ || v_bronze_columns || $cmd$
						FROM datasource
						ON CONFLICT ON CONSTRAINT "$cmd$ || v_bronze_columns_pk_name || $cmd$" DO NOTHING
						RETURNING $cmd$ || v_bronze_columns_pk || $cmd$
					)
					SELECT backfill_registry.insert_timestamp, COUNT(bronze_registry.*) 
					FROM bronze_registry, backfill_registry
					GROUP BY 1
				$cmd$;

				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd INTO v_insert_timestamp, v_qty;

					-- Atualizando a tabela bronze_backfill_control
					-- Se na query em loop a coluna bronze_layer_control_id view NULL, então é um novo registro para controlarmos
					IF v_record.bronze_layer_control_id IS NULL THEN
						INSERT INTO data_catalog.bronze_backfill_control (
							tb_databases_id
							, tb_schemas_id
							, tb_tables_id
							, payload_limit
							, target_timestamp
							, insert_timestamp
							, insert_done
							, created_at
						) VALUES (
							v_record.database_id
							, v_record.schema_id
							, v_record.table_id
							, v_payload_limit
							, v_target_timestamp
							, v_insert_timestamp
							, (CASE WHEN v_insert_timestamp >= v_target_timestamp THEN true ELSE false END)
							, clock_timestamp()
						);
					ELSIF COALESCE(v_qty,0) > 0 THEN
						UPDATE data_catalog.bronze_backfill_control SET
						insert_timestamp = COALESCE(v_insert_timestamp,insert_timestamp),
						insert_done = (CASE WHEN v_insert_timestamp >= v_target_timestamp THEN true ELSE false END),
						updated_at = clock_timestamp()
						WHERE id = v_record.bronze_layer_control_id;
					ELSIF COALESCE(v_qty,0) = 0 THEN
						UPDATE data_catalog.bronze_backfill_control SET
						insert_timestamp = COALESCE(v_insert_timestamp,insert_timestamp),
						insert_done = true,
						updated_at = clock_timestamp()
						WHERE id = v_record.bronze_layer_control_id;
					END IF;
				END IF;

				RETURN QUERY
					SELECT	bbc.id::INTEGER AS id
							, bbc.tb_databases_id::VARCHAR(32) AS tb_databases_id
							, bbc.tb_schemas_id::VARCHAR(32) AS tb_schemas_id
							, bbc.tb_tables_id::VARCHAR(32) AS tb_tables_id
							, null::VARCHAR(32) AS tb_columns_id
							, bbc.payload_limit::INTEGER AS payload_limit
							, bbc.target_timestamp::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
							, bbc.insert_timestamp::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
							, bbc.insert_done::BOOLEAN AS insert_done
							, bbc.update_timestamp::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
							, bbc.update_done::BOOLEAN AS update_done
							, bbc.delete_timestamp::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
							, bbc.delete_done::BOOLEAN AS delete_done
							, bbc.created_at::TIMESTAMP WITHOUT TIME ZONE AS created_at
							, bbc.updated_at::TIMESTAMP WITHOUT TIME ZONE AS updated_at
					FROM data_catalog.bronze_backfill_control bbc
					WHERE bbc.tb_databases_id = v_record.database_id
					AND bbc.tb_schemas_id = v_record.schema_id
					AND bbc.tb_tables_id = v_record.table_id
					AND bbc.tb_columns_id IS NULL;
				RETURN;
			END IF;
		END LOOP;

-- ======================================================================================================================================================
	-- Forçando fullcharge em determinada coluna de uma tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	-- [OBRIGATÓRIO] p_columns_id IS NOT NULL
	ELSIF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NOT NULL THEN

	END IF;

-- ======================================================================================================================================================
	-- Erro na relação dos parâmetros
	-- Retornamos uma query vazia
	RETURN QUERY
		SELECT	null::INTEGER AS id
				, null::VARCHAR(32) AS tb_databases_id
				, null::VARCHAR(32) AS tb_schemas_id
				, null::VARCHAR(32) AS tb_tables_id
				, null::VARCHAR(32) AS tb_columns_id
				, null::INTEGER AS payload_limit
				, null::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
				, null::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
				, null::BOOLEAN AS insert_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
				, null::BOOLEAN AS update_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
				, null::BOOLEAN AS delete_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS created_at
				, null::TIMESTAMP WITHOUT TIME ZONE AS updated_at;
		RETURN;
END; $BODY$;

CREATE OR REPLACE FUNCTION data_catalog.bronze_backfill_updates(p_databases_id VARCHAR DEFAULT NULL, p_schemas_id VARCHAR DEFAULT NULL, p_tables_id VARCHAR DEFAULT NULL, p_columns_id VARCHAR DEFAULT NULL)
RETURNS SETOF data_catalog.bronze_backfill_control
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
-- VARIÁVEIS AUXILIARES E DE CONTROLE
DECLARE v_record RECORD;
DECLARE v_cmd VARCHAR;
-- VARIAVEIS GLOBAIS
DECLARE v_payload_limit INTEGER;
DECLARE v_bronze_columns VARCHAR;
DECLARE v_hstlog_new_columns VARCHAR;
DECLARE v_hstlog_old_columns VARCHAR;
DECLARE v_bronze_columns_pk_name VARCHAR;
DECLARE v_bronze_columns_pk VARCHAR;
DECLARE v_target_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp();
DECLARE v_qty INTEGER DEFAULT 0;
-- VARIÁVEIS DE OPERAÇÕES DE UPDATE
DECLARE v_update_timestamp TIMESTAMP WITHOUT TIME ZONE;
DECLARE v_bronze_columns_not_pk VARCHAR;
DECLARE v_bronze_raw_columns VARCHAR;
DECLARE v_hstlog_columns_pk VARCHAR;
BEGIN
	-- Forçando fullcharge em determinada tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	IF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NULL THEN

		FOR v_record IN
			SELECT	DISTINCT
					vw.database_id
					, vw.database_name
					, vw.schema_id
					, vw.schema_name
					, vw.table_id
					, vw.table_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"') AS bronze_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"') AS raw_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"') AS hstlog_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"."',vw.table_name,'"') AS bronze_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"."',vw.table_name,'"') AS raw_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog"') AS hstlog_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_insert"') AS hstlog_insert_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_update"') AS hstlog_update_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_delete"') AS hstlog_delete_path
					, bbc.id AS bronze_layer_control_id
					, bbc.payload_limit
					, bbc.target_timestamp
					, bbc.insert_timestamp
					, bbc.insert_done
					, bbc.update_timestamp
					, bbc.update_done
					, bbc.delete_timestamp
					, bbc.delete_done
			FROM data_catalog.vw_catalog vw
			LEFT JOIN data_catalog.bronze_layer_control blc
				ON blc.tb_databases_id = vw.database_id
				AND blc.tb_schemas_id = vw.schema_id
				AND blc.tb_tables_id = vw.table_id
			LEFT JOIN data_catalog.bronze_backfill_control bbc
				ON bbc.tb_databases_id = vw.database_id
				AND bbc.tb_schemas_id = vw.schema_id
				AND bbc.tb_tables_id = vw.table_id
				AND bbc.tb_columns_id IS NULL
			WHERE vw.database_status_id = 5
			AND vw.schema_status_id = 5
			AND vw.table_status_id = 5
			AND blc.tb_tables_id IS NULL
			AND (
				bbc.id IS NULL
				OR bbc.update_done IS FALSE
			)
			AND vw.database_id = p_databases_id
			AND vw.schema_id = p_schemas_id
			AND vw.table_id = p_tables_id
		LOOP
			-- Setando a quantidade limite de dados dentro do chunk do fullcharge
			-- Caso não esteja previamente definido, precisaremos atualizar as estatísticas da tabela para pegar uma quantidade de 25% de n_live_tup
			IF v_record.payload_limit IS NULL THEN
				-- Atualizando as estatísticas para pegar 25% da tabela
				v_cmd := $cmd$ ANALYZE $cmd$ || v_record.raw_path;
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
				END IF;

				SELECT ROUND(n_live_tup * 0.25)::INTEGER 
				FROM pg_stat_user_tables 
				WHERE schemaname = REPLACE(v_record.raw_schema_name,'"','')
				AND relname = v_record.table_name
				INTO v_payload_limit;
			ELSE
				v_payload_limit := v_record.payload_limit;
			END IF;

			-- Adicionando à variável v_target_timestamp a data limite para o fullcharge
			-- Caso o valor retorne null da query em loop, deixaremos o default definido na variável
			IF v_record.target_timestamp IS NOT NULL THEN
				v_target_timestamp := v_record.target_timestamp;
			END IF;

			-- Coletando os nomes das colunas para uma operação (INSERT, UPDATE, DELETE) concisos e ordenados
			SELECT 	string_agg(CONCAT('"', column_name, '"'),', ') AS bronze_columns
					, string_agg(CONCAT('("',v_record.table_name,'_new"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_new_columns
					, string_agg(CONCAT('("',v_record.table_name,'_old"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_old_columns
			FROM (
				SELECT column_name, data_type
				FROM information_schema.columns
				WHERE table_schema = REPLACE(v_record.bronze_schema_name,'"','')
				AND table_name = v_record.table_name
				ORDER BY ordinal_position
			) columns
			INTO v_bronze_columns, v_hstlog_new_columns, v_hstlog_old_columns;

			-- Coletando os nomes das colunas que são PK para UPSERT
			SELECT pk_name, CONCAT('(',string_agg(CONCAT('"',column_name,'"'),', '),')') AS bronze_columns_pk
			FROM (
				SELECT tc.constraint_name AS pk_name, c.column_name, c.data_type
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
					ON kcu.table_schema = tc.table_schema
					AND kcu.table_name = tc.table_name
					AND kcu.constraint_name = tc.constraint_name
				JOIN information_schema.columns c
					ON c.table_schema = tc.table_schema
					AND c.table_name = tc.table_name
					AND c.column_name = kcu.column_name
				WHERE tc.table_schema = REPLACE(v_record.bronze_schema_name,'"','')
				AND tc.table_name = v_record.table_name
				AND tc.constraint_type = 'PRIMARY KEY'
			) columns_pk
			GROUP BY pk_name
			INTO v_bronze_columns_pk_name, v_bronze_columns_pk;
			
			-- ======================================================================================================================================================
			-- ======================================================================================================================================================
			-- ======================================================================================================================================================
			IF v_record.update_done IS NULL OR v_record.update_done IS FALSE THEN

				-- Coletando a data de referência para o chunk
				-- Caso esteja nula, a data utilizada será a primeira data encontrada na tabela histórica
				IF v_record.update_timestamp IS NULL THEN
					v_cmd := $cmd$ SELECT MIN(executed_at) FROM $cmd$ || v_record.hstlog_update_path;
					IF v_cmd IS NOT NULL THEN
						EXECUTE v_cmd INTO v_update_timestamp;
					END IF;
				ELSE
					v_update_timestamp := v_record.update_timestamp;
				END IF;

				-- Criando de forma dinâmica uma atribuição entre as colunas durante o update
				SELECT	CONCAT(string_agg(CONCAT('"',column_name,'"'),', ')) AS bronze_columns_not_pk
						, CONCAT(string_agg(CONCAT('"',column_name,'" = ds."',column_name,'"'),', ')) AS bronze_raw_columns
				FROM (
					SELECT tc.constraint_name AS pk_name, c.column_name, c.data_type
					FROM information_schema.columns c
					LEFT JOIN information_schema.table_constraints tc
						ON tc.table_schema = c.table_schema
						AND tc.table_name = c.table_name
					LEFT JOIN information_schema.key_column_usage kcu
						ON kcu.table_schema = tc.table_schema
						AND kcu.table_name = tc.table_name
						AND kcu.constraint_name = tc.constraint_name
						AND kcu.column_name = c.column_name
					WHERE tc.table_schema = REPLACE(v_record.bronze_schema_name,'"','')
					AND tc.table_name = v_record.table_name
					AND tc.constraint_type = 'PRIMARY KEY'
					AND kcu.column_name IS NULL
				) columns_not_pk
				INTO v_bronze_columns_not_pk, v_bronze_raw_columns;
	
				-- Criando de forma dinâmica uma comparação para ser usada na cláusula WHERE entre as PKs
				SELECT	CONCAT('(',string_agg(CONCAT('bronze."',column_name,'"'),', '),')') AS bronze_columns_pk
						, CONCAT('(',string_agg(CONCAT('ds."',column_name,'"'),', '),')') AS hstlog_columns_pk
				FROM (
					SELECT tc.constraint_name AS pk_name, c.column_name, c.data_type
					FROM information_schema.table_constraints tc
					JOIN information_schema.key_column_usage kcu
						ON kcu.table_schema = tc.table_schema
						AND kcu.table_name = tc.table_name
						AND kcu.constraint_name = tc.constraint_name
					JOIN information_schema.columns c
						ON c.table_schema = tc.table_schema
						AND c.table_name = tc.table_name
						AND c.column_name = kcu.column_name
					WHERE tc.table_schema = REPLACE(v_record.bronze_schema_name,'"','')
					AND tc.table_name = v_record.table_name
					AND tc.constraint_type = 'PRIMARY KEY'
				) columns_pk
				INTO v_bronze_columns_pk, v_hstlog_columns_pk;

				-- Carga do chunk fullcharge da tabela
				v_cmd := $cmd$
					WITH datasource AS (
						SELECT	$cmd$ || v_hstlog_new_columns || $cmd$
								, executed_at
						FROM $cmd$ || v_record.hstlog_update_path || $cmd$
						WHERE executed_at >= '$cmd$ || v_update_timestamp || $cmd$'
						AND executed_at < '$cmd$ || v_target_timestamp || $cmd$'
						ORDER BY executed_at ASC
						LIMIT $cmd$ || v_payload_limit || $cmd$
					), backfill_registry AS (
						SELECT MAX(ds.executed_at) AS update_timestamp
						FROM datasource ds
					), bronze_registry AS (
						UPDATE $cmd$ || v_record.bronze_path || $cmd$ bronze SET
						$cmd$ || v_bronze_raw_columns || $cmd$
						FROM datasource ds
						WHERE $cmd$ || v_bronze_columns_pk || $cmd$ = $cmd$ || v_hstlog_columns_pk || $cmd$
						RETURNING $cmd$ || v_bronze_columns_pk || $cmd$
					)
					SELECT backfill_registry.update_timestamp, COUNT(bronze_registry.*) 
					FROM bronze_registry, backfill_registry
					GROUP BY 1
				$cmd$;

				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd INTO v_update_timestamp, v_qty;

					-- Atualizando a tabela bronze_backfill_control
					-- Se na query em loop a coluna bronze_layer_control_id view NULL, então é um novo registro para controlarmos
					IF COALESCE(v_qty,0) > 0 THEN
						UPDATE data_catalog.bronze_backfill_control SET
						update_timestamp = COALESCE(v_update_timestamp,update_timestamp),
						update_done = (CASE WHEN v_update_timestamp >= v_update_timestamp THEN true ELSE false END),
						updated_at = clock_timestamp()
						WHERE id = v_record.bronze_layer_control_id;
					ELSIF COALESCE(v_qty,0) = 0 THEN
						UPDATE data_catalog.bronze_backfill_control SET
						update_timestamp = COALESCE(v_update_timestamp,update_timestamp),
						update_done = true,
						updated_at = clock_timestamp()
						WHERE id = v_record.bronze_layer_control_id;
					END IF;
				END IF;

				RETURN QUERY
					SELECT	bbc.id::INTEGER AS id
							, bbc.tb_databases_id::VARCHAR(32) AS tb_databases_id
							, bbc.tb_schemas_id::VARCHAR(32) AS tb_schemas_id
							, bbc.tb_tables_id::VARCHAR(32) AS tb_tables_id
							, null::VARCHAR(32) AS tb_columns_id
							, bbc.payload_limit::INTEGER AS payload_limit
							, bbc.target_timestamp::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
							, bbc.insert_timestamp::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
							, bbc.insert_done::BOOLEAN AS insert_done
							, bbc.update_timestamp::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
							, bbc.update_done::BOOLEAN AS update_done
							, bbc.delete_timestamp::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
							, bbc.delete_done::BOOLEAN AS delete_done
							, bbc.created_at::TIMESTAMP WITHOUT TIME ZONE AS created_at
							, bbc.updated_at::TIMESTAMP WITHOUT TIME ZONE AS updated_at
					FROM data_catalog.bronze_backfill_control bbc
					WHERE bbc.tb_databases_id = v_record.database_id
					AND bbc.tb_schemas_id = v_record.schema_id
					AND bbc.tb_tables_id = v_record.table_id
					AND bbc.tb_columns_id IS NULL;
				RETURN;
			END IF;
		END LOOP;

-- ======================================================================================================================================================
	-- Forçando fullcharge em determinada coluna de uma tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	-- [OBRIGATÓRIO] p_columns_id IS NOT NULL
	ELSIF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NOT NULL THEN

	END IF;

-- ======================================================================================================================================================
	-- Erro na relação dos parâmetros
	-- Retornamos uma query vazia
	RETURN QUERY
		SELECT	null::INTEGER AS id
				, null::VARCHAR(32) AS tb_databases_id
				, null::VARCHAR(32) AS tb_schemas_id
				, null::VARCHAR(32) AS tb_tables_id
				, null::VARCHAR(32) AS tb_columns_id
				, null::INTEGER AS payload_limit
				, null::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
				, null::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
				, null::BOOLEAN AS insert_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
				, null::BOOLEAN AS update_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
				, null::BOOLEAN AS delete_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS created_at
				, null::TIMESTAMP WITHOUT TIME ZONE AS updated_at;
		RETURN;
END; $BODY$;

CREATE OR REPLACE FUNCTION data_catalog.bronze_backfill_deletes(p_databases_id VARCHAR DEFAULT NULL, p_schemas_id VARCHAR DEFAULT NULL, p_tables_id VARCHAR DEFAULT NULL, p_columns_id VARCHAR DEFAULT NULL)
RETURNS SETOF data_catalog.bronze_backfill_control
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
-- VARIÁVEIS AUXILIARES E DE CONTROLE
DECLARE v_record RECORD;
DECLARE v_cmd VARCHAR;
-- VARIAVEIS GLOBAIS
DECLARE v_payload_limit INTEGER;
DECLARE v_bronze_columns VARCHAR;
DECLARE v_hstlog_new_columns VARCHAR;
DECLARE v_hstlog_old_columns VARCHAR;
DECLARE v_bronze_columns_pk_name VARCHAR;
DECLARE v_bronze_columns_pk VARCHAR;
DECLARE v_target_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT clock_timestamp();
DECLARE v_qty INTEGER DEFAULT 0;
-- VARIÁVEIS DE OPERAÇÕES DE DELETE
DECLARE v_delete_timestamp TIMESTAMP WITHOUT TIME ZONE;
DECLARE v_hstlog_columns_pk VARCHAR;
BEGIN
	-- Forçando fullcharge em determinada tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	IF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NULL THEN

		FOR v_record IN
			SELECT	DISTINCT
					vw.database_id
					, vw.database_name
					, vw.schema_id
					, vw.schema_name
					, vw.table_id
					, vw.table_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"') AS bronze_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"') AS raw_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"') AS hstlog_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"."',vw.table_name,'"') AS bronze_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"."',vw.table_name,'"') AS raw_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog"') AS hstlog_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_insert"') AS hstlog_insert_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_update"') AS hstlog_update_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_delete"') AS hstlog_delete_path
					, bbc.id AS bronze_layer_control_id
					, bbc.payload_limit
					, bbc.target_timestamp
					, bbc.insert_timestamp
					, bbc.insert_done
					, bbc.update_timestamp
					, bbc.update_done
					, bbc.delete_timestamp
					, bbc.delete_done
			FROM data_catalog.vw_catalog vw
			LEFT JOIN data_catalog.bronze_layer_control blc
				ON blc.tb_databases_id = vw.database_id
				AND blc.tb_schemas_id = vw.schema_id
				AND blc.tb_tables_id = vw.table_id
			LEFT JOIN data_catalog.bronze_backfill_control bbc
				ON bbc.tb_databases_id = vw.database_id
				AND bbc.tb_schemas_id = vw.schema_id
				AND bbc.tb_tables_id = vw.table_id
				AND bbc.tb_columns_id IS NULL
			WHERE vw.database_status_id = 5
			AND vw.schema_status_id = 5
			AND vw.table_status_id = 5
			AND blc.tb_tables_id IS NULL
			AND (
				bbc.id IS NULL
				OR bbc.delete_done IS FALSE
			)
			AND vw.database_id = p_databases_id
			AND vw.schema_id = p_schemas_id
			AND vw.table_id = p_tables_id
		LOOP
			-- Setando a quantidade limite de dados dentro do chunk do fullcharge
			-- Caso não esteja previamente definido, precisaremos atualizar as estatísticas da tabela para pegar uma quantidade de 25% de n_live_tup
			IF v_record.payload_limit IS NULL THEN
				-- Atualizando as estatísticas para pegar 25% da tabela
				v_cmd := $cmd$ ANALYZE $cmd$ || v_record.raw_path;
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
				END IF;

				SELECT ROUND(n_live_tup * 0.25)::INTEGER 
				FROM pg_stat_user_tables 
				WHERE schemaname = REPLACE(v_record.raw_schema_name,'"','')
				AND relname = v_record.table_name
				INTO v_payload_limit;
			ELSE
				v_payload_limit := v_record.payload_limit;
			END IF;

			-- Adicionando à variável v_target_timestamp a data limite para o fullcharge
			-- Caso o valor retorne null da query em loop, deixaremos o default definido na variável
			IF v_record.target_timestamp IS NOT NULL THEN
				v_target_timestamp := v_record.target_timestamp;
			END IF;

			-- Coletando os nomes das colunas para uma operação (INSERT, UPDATE, DELETE) concisos e ordenados
			SELECT 	string_agg(CONCAT('"', column_name, '"'),', ') AS bronze_columns
					, string_agg(CONCAT('("',v_record.table_name,'_new"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_new_columns
					, string_agg(CONCAT('("',v_record.table_name,'_old"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_old_columns
			FROM (
				SELECT column_name, data_type
				FROM information_schema.columns
				WHERE table_schema = REPLACE(v_record.bronze_schema_name,'"','')
				AND table_name = v_record.table_name
				ORDER BY ordinal_position
			) columns
			INTO v_bronze_columns, v_hstlog_new_columns, v_hstlog_old_columns;

			-- Coletando os nomes das colunas que são PK para UPSERT
			SELECT pk_name, CONCAT('(',string_agg(CONCAT('"',column_name,'"'),', '),')') AS bronze_columns_pk
			FROM (
				SELECT tc.constraint_name AS pk_name, c.column_name, c.data_type
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
					ON kcu.table_schema = tc.table_schema
					AND kcu.table_name = tc.table_name
					AND kcu.constraint_name = tc.constraint_name
				JOIN information_schema.columns c
					ON c.table_schema = tc.table_schema
					AND c.table_name = tc.table_name
					AND c.column_name = kcu.column_name
				WHERE tc.table_schema = REPLACE(v_record.bronze_schema_name,'"','')
				AND tc.table_name = v_record.table_name
				AND tc.constraint_type = 'PRIMARY KEY'
			) columns_pk
			GROUP BY pk_name
			INTO v_bronze_columns_pk_name, v_bronze_columns_pk;
			
			-- ======================================================================================================================================================
			-- ======================================================================================================================================================
			-- ======================================================================================================================================================
			IF v_record.delete_done IS NULL OR v_record.delete_done IS FALSE THEN

				-- Coletando a data de referência para o chunk
				-- Caso esteja nula, a data utilizada será a primeira data encontrada na tabela histórica
				IF v_record.delete_timestamp IS NULL THEN
					v_cmd := $cmd$ SELECT MIN(executed_at) FROM $cmd$ || v_record.hstlog_delete_path;
					IF v_cmd IS NOT NULL THEN
						EXECUTE v_cmd INTO v_delete_timestamp;
					END IF;
				ELSE
					v_delete_timestamp := v_record.delete_timestamp;
				END IF;

				-- RAISE EXCEPTION '%', v_delete_timestamp;

				-- Criando de forma dinâmica uma comparação para ser usada na cláusula WHERE entre as PKs
				SELECT	CONCAT('(',string_agg(CONCAT('bronze."',column_name,'"'),', '),')') AS bronze_columns_pk
						, CONCAT('(',string_agg(CONCAT('ds."',column_name,'"'),', '),')') AS hstlog_columns_pk
				FROM (
					SELECT tc.constraint_name AS pk_name, c.column_name, c.data_type
					FROM information_schema.table_constraints tc
					JOIN information_schema.key_column_usage kcu
						ON kcu.table_schema = tc.table_schema
						AND kcu.table_name = tc.table_name
						AND kcu.constraint_name = tc.constraint_name
					JOIN information_schema.columns c
						ON c.table_schema = tc.table_schema
						AND c.table_name = tc.table_name
						AND c.column_name = kcu.column_name
					WHERE tc.table_schema = REPLACE(v_record.bronze_schema_name,'"','')
					AND tc.table_name = v_record.table_name
					AND tc.constraint_type = 'PRIMARY KEY'
				) columns_pk
				INTO v_bronze_columns_pk, v_hstlog_columns_pk;

				-- Carga do chunk fullcharge da tabela
				v_cmd := $cmd$
					WITH datasource AS (
						SELECT	$cmd$ || v_hstlog_old_columns || $cmd$
								, executed_at
						FROM $cmd$ || v_record.hstlog_delete_path || $cmd$
						WHERE executed_at >= '$cmd$ || v_delete_timestamp || $cmd$'
						AND executed_at < '$cmd$ || v_target_timestamp || $cmd$'
						ORDER BY executed_at ASC
						LIMIT $cmd$ || v_payload_limit || $cmd$
					), backfill_registry AS (
						SELECT MAX(ds.executed_at) AS delete_timestamp
						FROM datasource ds
					), bronze_registry AS (
						DELETE FROM $cmd$ || v_record.bronze_path || $cmd$ bronze
						USING datasource ds
						WHERE $cmd$ || v_bronze_columns_pk || $cmd$ = $cmd$ || v_hstlog_columns_pk || $cmd$
						RETURNING $cmd$ || v_bronze_columns_pk || $cmd$
					)
					SELECT backfill_registry.delete_timestamp, COUNT(bronze_registry.*) 
					FROM bronze_registry, backfill_registry
					GROUP BY 1
				$cmd$;

				-- RAISE EXCEPTION '%', v_cmd;

				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd INTO v_delete_timestamp, v_qty;

					-- Atualizando a tabela bronze_backfill_control
					-- Se na query em loop a coluna bronze_layer_control_id view NULL, então é um novo registro para controlarmos
					IF COALESCE(v_qty,0) > 0 THEN
						UPDATE data_catalog.bronze_backfill_control SET
						delete_timestamp = COALESCE(v_delete_timestamp,delete_timestamp),
						delete_done = (CASE WHEN v_delete_timestamp >= v_delete_timestamp THEN true ELSE false END),
						updated_at = clock_timestamp()
						WHERE id = v_record.bronze_layer_control_id;
					ELSIF COALESCE(v_qty,0) = 0 THEN
						UPDATE data_catalog.bronze_backfill_control SET
						delete_timestamp = COALESCE(v_delete_timestamp,delete_timestamp),
						delete_done = true,
						updated_at = clock_timestamp()
						WHERE id = v_record.bronze_layer_control_id;
					END IF;
				END IF;

				RETURN QUERY
					SELECT	bbc.id::INTEGER AS id
							, bbc.tb_databases_id::VARCHAR(32) AS tb_databases_id
							, bbc.tb_schemas_id::VARCHAR(32) AS tb_schemas_id
							, bbc.tb_tables_id::VARCHAR(32) AS tb_tables_id
							, null::VARCHAR(32) AS tb_columns_id
							, bbc.payload_limit::INTEGER AS payload_limit
							, bbc.target_timestamp::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
							, bbc.insert_timestamp::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
							, bbc.insert_done::BOOLEAN AS insert_done
							, bbc.update_timestamp::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
							, bbc.update_done::BOOLEAN AS update_done
							, bbc.delete_timestamp::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
							, bbc.delete_done::BOOLEAN AS delete_done
							, bbc.created_at::TIMESTAMP WITHOUT TIME ZONE AS created_at
							, bbc.updated_at::TIMESTAMP WITHOUT TIME ZONE AS updated_at
					FROM data_catalog.bronze_backfill_control bbc
					WHERE bbc.tb_databases_id = v_record.database_id
					AND bbc.tb_schemas_id = v_record.schema_id
					AND bbc.tb_tables_id = v_record.table_id
					AND bbc.tb_columns_id IS NULL;
				RETURN;
			END IF;
		END LOOP;

-- ======================================================================================================================================================
	-- Forçando fullcharge em determinada coluna de uma tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	-- [OBRIGATÓRIO] p_columns_id IS NOT NULL
	ELSIF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NOT NULL THEN

	END IF;

-- ======================================================================================================================================================
	-- Erro na relação dos parâmetros
	-- Retornamos uma query vazia
	RETURN QUERY
		SELECT	null::INTEGER AS id
				, null::VARCHAR(32) AS tb_databases_id
				, null::VARCHAR(32) AS tb_schemas_id
				, null::VARCHAR(32) AS tb_tables_id
				, null::VARCHAR(32) AS tb_columns_id
				, null::INTEGER AS payload_limit
				, null::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
				, null::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
				, null::BOOLEAN AS insert_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
				, null::BOOLEAN AS update_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
				, null::BOOLEAN AS delete_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS created_at
				, null::TIMESTAMP WITHOUT TIME ZONE AS updated_at;
		RETURN;
END; $BODY$;

CREATE OR REPLACE FUNCTION data_catalog.backfill_done(p_databases_id VARCHAR DEFAULT NULL, p_schemas_id VARCHAR DEFAULT NULL, p_tables_id VARCHAR DEFAULT NULL, p_columns_id VARCHAR DEFAULT NULL)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
DECLARE v_backfill_done BOOLEAN DEFAULT false;
BEGIN
	-- CHECKING backfill_control está tudo como DONE
	-- Se SIM, insert into data_catalog.bronze_layer_control
	WITH backfill_done AS (
		SELECT	id
				, tb_databases_id
				, tb_schemas_id
				, tb_tables_id
				, target_timestamp
				, clock_timestamp() AS created_at
		FROM data_catalog.bronze_backfill_control
		WHERE tb_databases_id IS NOT DISTINCT FROM p_databases_id
		AND tb_schemas_id IS NOT DISTINCT FROM p_schemas_id
		AND tb_tables_id IS NOT DISTINCT FROM p_tables_id
		AND tb_columns_id IS NOT DISTINCT FROM p_columns_id
		AND insert_done IS TRUE
		AND update_done IS TRUE
		AND delete_done IS TRUE
	)
	, bronze_layer_control_add AS (
		INSERT INTO data_catalog.bronze_layer_control(
			tb_databases_id,
			tb_schemas_id,
			tb_tables_id,
			start_timestamp,
			insert_qty,
			update_qty,
			delete_qty,
			created_at
		)
		SELECT	tb_databases_id
				, tb_schemas_id
				, tb_tables_id
				, target_timestamp
				, 0
				, 0
				, 0
				, created_at
		FROM backfill_done
		RETURNING *
	)
	SELECT COALESCE((
		SELECT DISTINCT true
		FROM bronze_layer_control_add
	), false)
	INTO v_backfill_done;

	RETURN v_backfill_done;
END; $BODY$;

CREATE OR REPLACE FUNCTION data_catalog.bronze_backfill(p_databases_id VARCHAR DEFAULT NULL, p_schemas_id VARCHAR DEFAULT NULL, p_tables_id VARCHAR DEFAULT NULL, p_columns_id VARCHAR DEFAULT NULL)
RETURNS SETOF data_catalog.bronze_backfill_control
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
-- VARIÁVEIS AUXILIARES E DE CONTROLE
DECLARE v_record RECORD;
DECLARE v_cmd VARCHAR;
DECLARE v_backfill_done BOOLEAN;
BEGIN
	-- Forçando fullcharge em determinada tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	IF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NULL THEN

		FOR v_record IN
			SELECT	DISTINCT
					vw.database_id
					, vw.database_name
					, vw.schema_id
					, vw.schema_name
					, vw.table_id
					, vw.table_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"') AS bronze_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"') AS raw_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"') AS hstlog_schema_name
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'"."',vw.table_name,'"') AS bronze_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw"."',vw.table_name,'"') AS raw_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog"') AS hstlog_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_insert"') AS hstlog_insert_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_update"') AS hstlog_update_path
					, CONCAT('"',vw.database_name,'_',vw.schema_name,'_raw_hstlog"."',vw.table_name,'_hstlog_delete"') AS hstlog_delete_path
					, bbc.id AS bronze_layer_control_id
					, bbc.payload_limit
					, bbc.target_timestamp
					, bbc.insert_timestamp
					, bbc.insert_done
					, bbc.update_timestamp
					, bbc.update_done
					, bbc.delete_timestamp
					, bbc.delete_done
			FROM data_catalog.vw_catalog vw
			LEFT JOIN data_catalog.bronze_layer_control blc
				ON blc.tb_databases_id = vw.database_id
				AND blc.tb_schemas_id = vw.schema_id
				AND blc.tb_tables_id = vw.table_id
			LEFT JOIN data_catalog.bronze_backfill_control bbc
				ON bbc.tb_databases_id = vw.database_id
				AND bbc.tb_schemas_id = vw.schema_id
				AND bbc.tb_tables_id = vw.table_id
				AND bbc.tb_columns_id IS NULL
			WHERE vw.database_status_id = 5
			AND vw.schema_status_id = 5
			AND vw.table_status_id = 5
			AND blc.tb_tables_id IS NULL
			AND (
				bbc.id IS NULL OR (
					bbc.insert_done IS FALSE
					OR bbc.update_done IS FALSE
					OR bbc.delete_done IS FALSE
				)
			)
			AND vw.database_id = p_databases_id
			AND vw.schema_id = p_schemas_id
			AND vw.table_id = p_tables_id
		LOOP
			-- ======================================================================================================================================================
			IF v_record.insert_done IS NULL OR v_record.insert_done IS FALSE THEN
				RETURN QUERY
					SELECT	id::INTEGER AS id
							, tb_databases_id::VARCHAR(32) AS tb_databases_id
							, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
							, tb_tables_id::VARCHAR(32) AS tb_tables_id
							, tb_columns_id::VARCHAR(32) AS tb_columns_id
							, payload_limit::INTEGER AS payload_limit
							, target_timestamp::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
							, insert_timestamp::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
							, insert_done::BOOLEAN AS insert_done
							, update_timestamp::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
							, update_done::BOOLEAN AS update_done
							, delete_timestamp::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
							, delete_done::BOOLEAN AS delete_done
							, created_at::TIMESTAMP WITHOUT TIME ZONE AS created_at
							, updated_at::TIMESTAMP WITHOUT TIME ZONE AS updated_at
						FROM data_catalog.bronze_backfill_inserts(
							p_databases_id,
							p_schemas_id,
							p_tables_id,
							NULL
						);

			-- ======================================================================================================================================================
			ELSIF v_record.update_done IS NULL OR v_record.update_done IS FALSE THEN
				RETURN QUERY
					SELECT	id::INTEGER AS id
							, tb_databases_id::VARCHAR(32) AS tb_databases_id
							, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
							, tb_tables_id::VARCHAR(32) AS tb_tables_id
							, tb_columns_id::VARCHAR(32) AS tb_columns_id
							, payload_limit::INTEGER AS payload_limit
							, target_timestamp::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
							, insert_timestamp::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
							, insert_done::BOOLEAN AS insert_done
							, update_timestamp::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
							, update_done::BOOLEAN AS update_done
							, delete_timestamp::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
							, delete_done::BOOLEAN AS delete_done
							, created_at::TIMESTAMP WITHOUT TIME ZONE AS created_at
							, updated_at::TIMESTAMP WITHOUT TIME ZONE AS updated_at
						FROM data_catalog.bronze_backfill_updates(
							p_databases_id,
							p_schemas_id,
							p_tables_id,
							NULL
						);

			-- ======================================================================================================================================================
			ELSIF v_record.delete_done IS NULL OR v_record.delete_done IS FALSE THEN
				RETURN QUERY
					SELECT	id::INTEGER AS id
							, tb_databases_id::VARCHAR(32) AS tb_databases_id
							, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
							, tb_tables_id::VARCHAR(32) AS tb_tables_id
							, tb_columns_id::VARCHAR(32) AS tb_columns_id
							, payload_limit::INTEGER AS payload_limit
							, target_timestamp::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
							, insert_timestamp::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
							, insert_done::BOOLEAN AS insert_done
							, update_timestamp::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
							, update_done::BOOLEAN AS update_done
							, delete_timestamp::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
							, delete_done::BOOLEAN AS delete_done
							, created_at::TIMESTAMP WITHOUT TIME ZONE AS created_at
							, updated_at::TIMESTAMP WITHOUT TIME ZONE AS updated_at
						FROM data_catalog.bronze_backfill_deletes(
							p_databases_id,
							p_schemas_id,
							p_tables_id,
							NULL
						);
			END IF;

			SELECT backfill_done
			FROM data_catalog.backfill_done(p_databases_id, p_schemas_id, p_tables_id, null) backfill_done
			INTO v_backfill_done;

			RETURN;
			
		END LOOP;

-- ======================================================================================================================================================
	-- Forçando fullcharge em determinada coluna de uma tabela
	-- [OBRIGATÓRIO] p_databases_id IS NOT NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NOT NULL
	-- [OBRIGATÓRIO] p_tables_id IS NOT NULL
	-- [OBRIGATÓRIO] p_columns_id IS NOT NULL
	ELSIF p_databases_id IS NOT NULL AND p_schemas_id IS NOT NULL AND p_tables_id IS NOT NULL AND p_columns_id IS NOT NULL THEN

-- ======================================================================================================================================================
	-- Loop em todas as tabelas que precisam de fullcharge
	-- [OBRIGATÓRIO] p_databases_id IS NULL
	-- [OBRIGATÓRIO] p_schemas_id IS NULL
	-- [OBRIGATÓRIO] p_tables_id IS NULL
	-- [OBRIGATÓRIO] p_columns_id IS NULL
	ELSIF p_databases_id IS NULL AND p_schemas_id IS NULL AND p_tables_id IS NULL AND p_columns_id IS NULL THEN

	END IF;

-- ======================================================================================================================================================
	-- Erro na relação dos parâmetros
	-- Retornamos uma query vazia
	RETURN QUERY
		SELECT	null::INTEGER AS id
				, null::VARCHAR(32) AS tb_databases_id
				, null::VARCHAR(32) AS tb_schemas_id
				, null::VARCHAR(32) AS tb_tables_id
				, null::VARCHAR(32) AS tb_columns_id
				, null::INTEGER AS payload_limit
				, null::TIMESTAMP WITHOUT TIME ZONE AS target_timestamp
				, null::TIMESTAMP WITHOUT TIME ZONE AS insert_timestamp
				, null::BOOLEAN AS insert_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS update_timestamp
				, null::BOOLEAN AS update_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS delete_timestamp
				, null::BOOLEAN AS delete_done
				, null::TIMESTAMP WITHOUT TIME ZONE AS created_at
				, null::TIMESTAMP WITHOUT TIME ZONE AS updated_at;
		RETURN;
END; $BODY$;

