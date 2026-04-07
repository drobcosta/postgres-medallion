CREATE SEQUENCE IF NOT EXISTS data_catalog.bronze_payload_erros_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

CREATE TABLE IF NOT EXISTS data_catalog.bronze_payload_erros
(
    id bigint NOT NULL DEFAULT nextval('data_catalog.bronze_payload_erros_id_seq'::regclass),
    tb_databases_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_schemas_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_tables_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
	op character varying(32) NOT NULL,
    error_message text COLLATE pg_catalog."default",
    error_detail text COLLATE pg_catalog."default",
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT bronze_payload_erros_pkey PRIMARY KEY (id)
);

COMMENT ON TABLE data_catalog.bronze_payload_erros
    IS 'Tabela responsável por armazenar os erros provenientes do processo de payload entre camada raw e camada bronze';

COMMENT ON COLUMN data_catalog.bronze_payload_erros.id
    IS 'Coluna PK da tabela.';

COMMENT ON COLUMN data_catalog.bronze_payload_erros.tb_databases_id
    IS 'Coluna responsável por registrar a qual database este error pertence. Não tem uma FK por se tratar de uma tabela de log.';

COMMENT ON COLUMN data_catalog.bronze_payload_erros.tb_schemas_id
    IS 'Coluna responsável por registrar a qual schema este error pertence. Não tem uma FK por se tratar de uma tabela de log.';

COMMENT ON COLUMN data_catalog.bronze_payload_erros.tb_tables_id
    IS 'Coluna responsável por registrar a qual table este error pertence. Não tem uma FK por se tratar de uma tabela de log.';

COMMENT ON COLUMN data_catalog.bronze_payload_erros.error_message
    IS 'Coluna responsável por armazenar a mensagem de erro do processo de payload.';

COMMENT ON COLUMN data_catalog.bronze_payload_erros.error_detail
    IS 'Coluna responsável por armazenar o detalhe do erro do processo de payload';

COMMENT ON COLUMN data_catalog.bronze_payload_erros.created_at
    IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';

CREATE OR REPLACE FUNCTION data_catalog.bronze_payload_deletes(
	p_databases_id character varying,
	p_schemas_id character varying,
	p_tables_id character varying)
    RETURNS SETOF data_catalog.bronze_payload_control 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE v_record RECORD;
DECLARE v_cmd VARCHAR;
DECLARE v_minutes_safety_margin INTEGER;
DECLARE v_target_timestamp TIMESTAMP;
DECLARE v_target_timestamp_safety_margin TIMESTAMP;
DECLARE v_hstlog_columns_only_pk VARCHAR;
DECLARE v_bronze_columns_pk VARCHAR;
DECLARE v_hstlog_columns_pk VARCHAR;
DECLARE v_qty INTEGER DEFAULT 0;
BEGIN
	-- OBRIGAÇÃO EM PASSAR TODOS OS PARÂMETROS PREENCHIDOS
	IF p_databases_id IS NULL OR p_schemas_id IS NULL OR p_tables_id IS NULL THEN
		RETURN QUERY
			SELECT	null::VARCHAR(32) AS tb_databases_id
					, null::VARCHAR(32) AS tb_schemas_id
					, null::VARCHAR(32) AS tb_tables_id
					, null::TIMESTAMP AS insert_timestamp
					, null::BIGINT AS insert_qty
					, null::TIMESTAMP AS update_timestamp
					, null::BIGINT AS update_qty
					, null::TIMESTAMP AS delete_timestamp
					, null::BIGINT AS delete_qty
					, null::TIMESTAMP AS created_at
					, null::TIMESTAMP AS updated_at;
		RETURN;
	END IF;

	-- USANDO LOOP PARA NÃO PRECISAR CRIAR DIVERSAS VARIÁVEIS
	-- ASSIM CRIAMOS APENAS UMA VARIÁVEL DO TIPO RECORD COM TODOS OS DADOS QUE PRECISAMOS DE FORMA DINÂMICA
	FOR v_record IN
		SELECT	DISTINCT
				vw.database_id
				, vw.database_name
				, vw.schema_id
				, vw.schema_name
				, vw.table_id
				, vw.table_name
				, vw.bronze_schema_name
				, vw.raw_schema_name
				, vw.hstlog_schema_name
				, vw.bronze_path
				, vw.raw_path
				, vw.hstlog_path
				, vw.hstlog_insert_path
				, vw.hstlog_update_path
				, vw.hstlog_delete_path
				, bpc.delete_timestamp AS target_timestamp
				, vw.table_payload_period_minutes
		FROM data_catalog.vw_catalog vw
		JOIN data_catalog.bronze_payload_control bpc
			ON bpc.tb_databases_id = vw.database_id
			AND bpc.tb_schemas_id = vw.schema_id
			AND bpc.tb_tables_id = vw.table_id
		WHERE vw.database_status_id = 5
		AND vw.schema_status_id = 5
		AND vw.table_status_id = 5
		AND vw.database_id = p_databases_id
		AND vw.schema_id = p_schemas_id
		AND vw.table_id = p_tables_id
	LOOP
		-- SE NÃO EXISTIR O VALOR DE TARGET_TIMESTAMP (INSERT_TIMESTAMP) NA TABELA
		-- É PORQUE TEM ALGUM ERRO AÍ.
		-- ENTÃO A GENTE RETORNA UMA QUERY NULA
		v_target_timestamp := v_record.target_timestamp;
		IF v_target_timestamp IS NULL THEN
			RETURN QUERY
				SELECT	null::VARCHAR(32) AS tb_databases_id
						, null::VARCHAR(32) AS tb_schemas_id
						, null::VARCHAR(32) AS tb_tables_id
						, null::TIMESTAMP AS insert_timestamp
						, null::BIGINT AS insert_qty
						, null::TIMESTAMP AS update_timestamp
						, null::BIGINT AS update_qty
						, null::TIMESTAMP AS delete_timestamp
						, null::BIGINT AS delete_qty
						, null::TIMESTAMP AS created_at
						, null::TIMESTAMP AS updated_at;
			RETURN;
		END IF;

		-- CRIAÇÃO DE UMA VARIÁVEL CONTENDO A DATA DO ÚLTIMO PROCESSAMENTO DE COMANDOS INSERTS
		-- PORÉM NÃO VAMOS USAR A DATA DE FORMA EXATA
		-- VAMOS CRIAR UMA MARGEM DE SEGURANÇA PARA PEGAR OS DADOS E FAZER UMA OPERAÇÃO DE UPSERT
		-- ESSA MARGEM DE SEGURANÇA SERÁ COM BASE NO TEMPO DO PERÍODO DE PAYLOAD DA TABELA
		-- POR EXEMPLO, SE A TABELA POSSUI O TB_PAYLOAD_PERIOD_ID A CADA 5 MINUTOS,
		-- DEVEREMOS PEGAR OS DADOS DA TABELA DE INSERTS A CADA 5 MINUTOS + 5% DESTE TEMPO ARREDONDADO PARA CIMA
		-- OU SEJA, AO INVÉS DE PEGAR OS REGISTROS A CADA 5 MINUTOS, PEGAREMOS A CADA 6 MINUTOS (5% DE 5 É 0,25 QUE ARREDONDADO PARA CIMA DÁ 1, OU SEJA, 5 + 1 = 6)
		v_minutes_safety_margin := CEILING(v_record.table_payload_period_minutes * 0.05);
		v_target_timestamp_safety_margin := v_target_timestamp - (v_minutes_safety_margin * INTERVAL '1 minutes');

		/*
			APÓS A CRIAÇÃO DAS VARIÁVEIS ABAIXO, QUE SERÃO USADAS COMO ATALHOS PARA A QUERY DINÂMCIA QUE SERÁ FORMA DENTRO DA VARIÁVEL V_CMD
			O RESULTADO DA QUERY DINÂMICA DEVERÁ SER SEMELHANTE À QUERY ABAIXO:
			
			WITH datasource AS (
				SELECT [table_name]_new->>'colunaPK' AS "colunaPK", executed_at
				FROM [database_name]_[schema_name]_raw_hstlog.[table_name]_hstlog_insert
				WHERE executed_at::TIMESTAMP(0) >= v_target_timestamp_safety_margin
				ORDER BY executed_at ASC
			)
			, new_target_timestamp AS (
				SELECT MAX(executed_at) AS executed_at
				FROM datasource
			)
			, op AS (
				DELETE FROM [database_name]_[schema_name].[table_name] bronze
				USING datasource ds
				WHERE ds.[columns_pk] = bronze.[columns_pk]
				RETURNING [columns_pk]
			)
			SELECT ntt.executed_at, COUNT(op.*)
			FROM new_target_timestamp ntt, op
			GROUP BY ntt.executed_at
		*/
		
		-- BRONZE PK COLUMNS AND HSTLOG PK COLUMNS
		SELECT	string_agg(CONCAT('("',table_name,'_old"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_columns_only_pk
				, CONCAT('(',string_agg(CONCAT('bronze."',column_name,'"'),', '),')') AS bronze_columns_pk
				, CONCAT('(',string_agg(CONCAT('ds."',column_name,'"'),', '),')') AS hstlog_columns_pk
		FROM (
			SELECT tc.constraint_name AS pk_name, c.table_name, c.column_name, c.data_type
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
		INTO v_hstlog_columns_only_pk, v_bronze_columns_pk, v_hstlog_columns_pk;

		-- PAYLOAD
		v_cmd := $cmd$
			WITH datasource AS (
				SELECT	$cmd$ || v_hstlog_columns_only_pk || $cmd$
						, executed_at
				FROM $cmd$ || v_record.hstlog_delete_path || $cmd$
				WHERE executed_at::TIMESTAMP(0) >= '$cmd$ || v_target_timestamp_safety_margin || $cmd$'
				ORDER BY executed_at ASC
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

		-- SE O PAYLOAD FOR CONCLUÍDO COM SUCESSO, ATUALIZAREMOS A TABELA DATA_CATALOG.BRONZE_PAYLOAD_CONTROL
		IF v_cmd IS NOT NULL THEN
			EXECUTE v_cmd INTO v_target_timestamp, v_qty;
			
			UPDATE data_catalog.bronze_payload_control SET
			delete_timestamp = COALESCE(v_target_timestamp,delete_timestamp),
			delete_qty = COALESCE(v_qty,0),
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id;

			RETURN QUERY
				SELECT	tb_databases_id::VARCHAR(32) AS tb_databases_id
						, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
						, tb_tables_id::VARCHAR(32) AS tb_tables_id
						, insert_timestamp::TIMESTAMP AS insert_timestamp
						, insert_qty::BIGINT AS insert_qty
						, update_timestamp::TIMESTAMP AS update_timestamp
						, update_qty::BIGINT AS update_qty
						, delete_timestamp::TIMESTAMP AS delete_timestamp
						, delete_qty::BIGINT AS delete_qty
						, created_at::TIMESTAMP AS created_at
						, updated_at::TIMESTAMP AS updated_at
				FROM data_catalog.bronze_payload_control
				WHERE tb_databases_id = v_record.database_id
				AND tb_schemas_id = v_record.schema_id
				AND tb_tables_id = v_record.table_id;
			RETURN;
		END IF;
	END LOOP;

	-- CASO TUDO DÊ ERRO, RETORNAMOS UMA QUERY NULA
	RETURN QUERY
		SELECT	null::VARCHAR(32) AS tb_databases_id
				, null::VARCHAR(32) AS tb_schemas_id
				, null::VARCHAR(32) AS tb_tables_id
				, null::TIMESTAMP AS insert_timestamp
				, null::BIGINT AS insert_qty
				, null::TIMESTAMP AS update_timestamp
				, null::BIGINT AS update_qty
				, null::TIMESTAMP AS delete_timestamp
				, null::BIGINT AS delete_qty
				, null::TIMESTAMP AS created_at
				, null::TIMESTAMP AS updated_at;
	RETURN;
	
END; 
$BODY$;

CREATE OR REPLACE FUNCTION data_catalog.bronze_payload_updates(
	p_databases_id character varying,
	p_schemas_id character varying,
	p_tables_id character varying)
    RETURNS SETOF data_catalog.bronze_payload_control 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE v_record RECORD;
DECLARE v_cmd VARCHAR;
DECLARE v_minutes_safety_margin INTEGER;
DECLARE v_target_timestamp TIMESTAMP;
DECLARE v_target_timestamp_safety_margin TIMESTAMP;
DECLARE v_hstlog_columns VARCHAR;
DECLARE v_hstlog_columns_old VARCHAR;
DECLARE v_bronze_columns_pk VARCHAR;
DECLARE v_hstlog_columns_pk VARCHAR;
DECLARE v_bronze_raw_columns VARCHAR;

DECLARE v_qty INTEGER DEFAULT 0;
BEGIN
	-- OBRIGAÇÃO EM PASSAR TODOS OS PARÂMETROS PREENCHIDOS
	IF p_databases_id IS NULL OR p_schemas_id IS NULL OR p_tables_id IS NULL THEN
		RETURN QUERY
			SELECT	null::VARCHAR(32) AS tb_databases_id
					, null::VARCHAR(32) AS tb_schemas_id
					, null::VARCHAR(32) AS tb_tables_id
					, null::TIMESTAMP AS insert_timestamp
					, null::BIGINT AS insert_qty
					, null::TIMESTAMP AS update_timestamp
					, null::BIGINT AS update_qty
					, null::TIMESTAMP AS delete_timestamp
					, null::BIGINT AS delete_qty
					, null::TIMESTAMP AS created_at
					, null::TIMESTAMP AS updated_at;
		RETURN;
	END IF;

	-- USANDO LOOP PARA NÃO PRECISAR CRIAR DIVERSAS VARIÁVEIS
	-- ASSIM CRIAMOS APENAS UMA VARIÁVEL DO TIPO RECORD COM TODOS OS DADOS QUE PRECISAMOS DE FORMA DINÂMICA
	FOR v_record IN
		SELECT	DISTINCT
				vw.database_id
				, vw.database_name
				, vw.schema_id
				, vw.schema_name
				, vw.table_id
				, vw.table_name
				, vw.bronze_schema_name
				, vw.raw_schema_name
				, vw.hstlog_schema_name
				, vw.bronze_path
				, vw.raw_path
				, vw.hstlog_path
				, vw.hstlog_insert_path
				, vw.hstlog_update_path
				, vw.hstlog_delete_path
				, bpc.update_timestamp AS target_timestamp
				, vw.table_payload_period_minutes
		FROM data_catalog.vw_catalog vw
		JOIN data_catalog.bronze_payload_control bpc
			ON bpc.tb_databases_id = vw.database_id
			AND bpc.tb_schemas_id = vw.schema_id
			AND bpc.tb_tables_id = vw.table_id
		WHERE vw.database_status_id = 5
		AND vw.schema_status_id = 5
		AND vw.table_status_id = 5
		AND vw.database_id = p_databases_id
		AND vw.schema_id = p_schemas_id
		AND vw.table_id = p_tables_id
	LOOP
		-- SE NÃO EXISTIR O VALOR DE TARGET_TIMESTAMP (INSERT_TIMESTAMP) NA TABELA
		-- É PORQUE TEM ALGUM ERRO AÍ.
		-- ENTÃO A GENTE RETORNA UMA QUERY NULA
		v_target_timestamp := v_record.target_timestamp;
		IF v_target_timestamp IS NULL THEN
			RETURN QUERY
				SELECT	null::VARCHAR(32) AS tb_databases_id
						, null::VARCHAR(32) AS tb_schemas_id
						, null::VARCHAR(32) AS tb_tables_id
						, null::TIMESTAMP AS insert_timestamp
						, null::BIGINT AS insert_qty
						, null::TIMESTAMP AS update_timestamp
						, null::BIGINT AS update_qty
						, null::TIMESTAMP AS delete_timestamp
						, null::BIGINT AS delete_qty
						, null::TIMESTAMP AS created_at
						, null::TIMESTAMP AS updated_at;
			RETURN;
		END IF;

		-- CRIAÇÃO DE UMA VARIÁVEL CONTENDO A DATA DO ÚLTIMO PROCESSAMENTO DE COMANDOS INSERTS
		-- PORÉM NÃO VAMOS USAR A DATA DE FORMA EXATA
		-- VAMOS CRIAR UMA MARGEM DE SEGURANÇA PARA PEGAR OS DADOS E FAZER UMA OPERAÇÃO DE UPSERT
		-- ESSA MARGEM DE SEGURANÇA SERÁ COM BASE NO TEMPO DO PERÍODO DE PAYLOAD DA TABELA
		-- POR EXEMPLO, SE A TABELA POSSUI O TB_PAYLOAD_PERIOD_ID A CADA 5 MINUTOS,
		-- DEVEREMOS PEGAR OS DADOS DA TABELA DE INSERTS A CADA 5 MINUTOS + 5% DESTE TEMPO ARREDONDADO PARA CIMA
		-- OU SEJA, AO INVÉS DE PEGAR OS REGISTROS A CADA 5 MINUTOS, PEGAREMOS A CADA 6 MINUTOS (5% DE 5 É 0,25 QUE ARREDONDADO PARA CIMA DÁ 1, OU SEJA, 5 + 1 = 6)
		v_minutes_safety_margin := CEILING(v_record.table_payload_period_minutes * 0.05);
		v_target_timestamp_safety_margin := v_target_timestamp - (v_minutes_safety_margin * INTERVAL '1 minutes');

		/*
			APÓS A CRIAÇÃO DAS VARIÁVEIS ABAIXO, QUE SERÃO USADAS COMO ATALHOS PARA A QUERY DINÂMCIA QUE SERÁ FORMA DENTRO DA VARIÁVEL V_CMD
			O RESULTADO DA QUERY DINÂMICA DEVERÁ SER SEMELHANTE À QUERY ABAIXO:
			
			WITH datasource AS (
				SELECT [table_name]_new->>'coluna1' AS "coluna1", [table_name]_new->>'coluna2' AS "coluna1", [table_name]_new->>'colunaN' AS "colunaN", executed_at
				FROM [database_name]_[schema_name]_raw_hstlog.[table_name]_hstlog_update
				WHERE executed_at::TIMESTAMP(0) >= v_target_timestamp_safety_margin
				ORDER BY executed_at ASC
			)
			, new_target_timestamp AS (
				SELECT MAX(executed_at) AS executed_at
				FROM datasource
			)
			, op AS (
				UPDATE [database_name]_[schema_name].[table_name] bronze SET
				"coluna1" = ds."coluna1",
				"coluna2" = ds."coluna2",
				"colunaN" = ds."colunaN",
				FROM datasource ds
				WHERE bronze.[columns_pk] = ds.[columns_pk]
				RETURNING [columns_pk]
			)
			SELECT ntt.executed_at, COUNT(op.*)
			FROM new_target_timestamp ntt, op
			GROUP BY ntt.executed_at
		*/

		-- HSTLOG COLUMNS
		SELECT	string_agg(CONCAT('("',table_name,'_new"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_columns
				, string_agg(CONCAT('("',table_name,'_old"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_columns_old
		FROM (
			SELECT table_name, column_name, data_type
			FROM information_schema.columns
			WHERE table_schema = REPLACE(v_record.bronze_schema_name,'"','')
			AND table_name = v_record.table_name
			ORDER BY ordinal_position
		) columns
		INTO v_hstlog_columns, v_hstlog_columns_old;

		-- GERANDO DINAMICAMENTE UMA ATRIBUIÇÃO ENTRE AS COLUNAS COM ALIAS DA QUERY DE UPDATE
		-- COLUNA1 = DS.COLUNA1
		-- RESULTADO ESPERADO NA QUERY DINÂMICA:
		-- 		UPDATE tabela bronze SET coluna1 = ds.coluna1, coluna2 = ds.coluna2
		SELECT CONCAT(string_agg(CONCAT('"',column_name,'" = ds."',column_name,'"'),', ')) AS bronze_raw_columns
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
		INTO v_bronze_raw_columns;

		-- PK COLUMNS FOR UPSERT
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

		-- PAYLOAD
		v_cmd := $cmd$
			WITH datasource AS (
				SELECT	$cmd$ || v_hstlog_columns || $cmd$
						, executed_at
				FROM $cmd$ || v_record.hstlog_update_path || $cmd$
				WHERE executed_at::TIMESTAMP(0) >= '$cmd$ || v_target_timestamp_safety_margin || $cmd$'
				ORDER BY executed_at ASC
			)
			, datasource_old AS (
				SELECT	$cmd$ || REPLACE(v_hstlog_columns,'_new"->>','_old"->>') || $cmd$
						, executed_at
				FROM $cmd$ || v_record.hstlog_update_path || $cmd$
				WHERE executed_at::TIMESTAMP(0) >= '$cmd$ || v_target_timestamp_safety_margin || $cmd$'
				ORDER BY executed_at ASC
			), new_target_timestamp AS (
				SELECT MAX(executed_at) AS executed_at
				FROM datasource
			), op AS (
				UPDATE $cmd$ || v_record.bronze_path || $cmd$ bronze SET
				$cmd$ || v_bronze_raw_columns || $cmd$
				FROM datasource ds
				JOIN datasource_old ds_old
					ON $cmd$ || v_hstlog_columns_pk || $cmd$ = $cmd$ || REPLACE(v_hstlog_columns_pk,'ds.','ds_old.') || $cmd$
				WHERE $cmd$ || v_bronze_columns_pk || $cmd$ = $cmd$ || v_hstlog_columns_pk || $cmd$
				AND bronze.$cmd$|| REPLACE(REPLACE(v_bronze_raw_columns,'ds.','ds_old.'),', ',' AND bronze.') || $cmd$
				RETURNING $cmd$ || v_bronze_columns_pk || $cmd$
			)
			SELECT ntt.executed_at, COUNT(op.*)
			FROM new_target_timestamp ntt, op
			GROUP BY ntt.executed_at
		$cmd$;

		-- SE O PAYLOAD FOR CONCLUÍDO COM SUCESSO, ATUALIZAREMOS A TABELA DATA_CATALOG.BRONZE_PAYLOAD_CONTROL
		IF v_cmd IS NOT NULL THEN
			EXECUTE v_cmd INTO v_target_timestamp, v_qty;
			
			UPDATE data_catalog.bronze_payload_control SET
			update_timestamp = COALESCE(v_target_timestamp,update_timestamp),
			update_qty = COALESCE(v_qty,0),
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id;

			RETURN QUERY
				SELECT	tb_databases_id::VARCHAR(32) AS tb_databases_id
						, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
						, tb_tables_id::VARCHAR(32) AS tb_tables_id
						, insert_timestamp::TIMESTAMP AS insert_timestamp
						, insert_qty::BIGINT AS insert_qty
						, update_timestamp::TIMESTAMP AS update_timestamp
						, update_qty::BIGINT AS update_qty
						, delete_timestamp::TIMESTAMP AS delete_timestamp
						, delete_qty::BIGINT AS delete_qty
						, created_at::TIMESTAMP AS created_at
						, updated_at::TIMESTAMP AS updated_at
				FROM data_catalog.bronze_payload_control
				WHERE tb_databases_id = v_record.database_id
				AND tb_schemas_id = v_record.schema_id
				AND tb_tables_id = v_record.table_id;
			RETURN;
		END IF;
	END LOOP;

	-- CASO TUDO DÊ ERRO, RETORNAMOS UMA QUERY NULA
	RETURN QUERY
		SELECT	null::VARCHAR(32) AS tb_databases_id
				, null::VARCHAR(32) AS tb_schemas_id
				, null::VARCHAR(32) AS tb_tables_id
				, null::TIMESTAMP AS insert_timestamp
				, null::BIGINT AS insert_qty
				, null::TIMESTAMP AS update_timestamp
				, null::BIGINT AS update_qty
				, null::TIMESTAMP AS delete_timestamp
				, null::BIGINT AS delete_qty
				, null::TIMESTAMP AS created_at
				, null::TIMESTAMP AS updated_at;
	RETURN;
	
END; 
$BODY$;

CREATE OR REPLACE FUNCTION data_catalog.bronze_payload_inserts(
	p_databases_id character varying,
	p_schemas_id character varying,
	p_tables_id character varying)
    RETURNS SETOF data_catalog.bronze_payload_control 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE v_record RECORD;
DECLARE v_cmd VARCHAR;
DECLARE v_minutes_safety_margin INTEGER;
DECLARE v_target_timestamp TIMESTAMP;
DECLARE v_target_timestamp_safety_margin TIMESTAMP;
DECLARE v_bronze_columns VARCHAR;
DECLARE v_hstlog_new_columns VARCHAR;
DECLARE v_bronze_columns_pk_name VARCHAR;
DECLARE v_bronze_columns_pk VARCHAR;
DECLARE v_qty INTEGER DEFAULT 0;
BEGIN
	-- OBRIGAÇÃO EM PASSAR TODOS OS PARÂMETROS PREENCHIDOS
	IF p_databases_id IS NULL OR p_schemas_id IS NULL OR p_tables_id IS NULL THEN
		RETURN QUERY
			SELECT	null::VARCHAR(32) AS tb_databases_id
					, null::VARCHAR(32) AS tb_schemas_id
					, null::VARCHAR(32) AS tb_tables_id
					, null::TIMESTAMP
					, null::BIGINT AS insert_qty
					, null::BIGINT AS update_qty
					, null::BIGINT AS delete_qty
					, null::TIMESTAMP AS created_at
					, null::TIMESTAMP AS updated_at;
		RETURN;
	END IF;

	-- USANDO LOOP PARA NÃO PRECISAR CRIAR DIVERSAS VARIÁVEIS
	-- ASSIM CRIAMOS APENAS UMA VARIÁVEL DO TIPO RECORD COM TODOS OS DADOS QUE PRECISAMOS DE FORMA DINÂMICA
	FOR v_record IN
		SELECT	DISTINCT
				vw.database_id
				, vw.database_name
				, vw.schema_id
				, vw.schema_name
				, vw.table_id
				, vw.table_name
				, vw.bronze_schema_name
				, vw.raw_schema_name
				, vw.hstlog_schema_name
				, vw.bronze_path
				, vw.raw_path
				, vw.hstlog_path
				, vw.hstlog_insert_path
				, vw.hstlog_update_path
				, vw.hstlog_delete_path
				, bpc.insert_timestamp AS target_timestamp
				, vw.table_payload_period_minutes
		FROM data_catalog.vw_catalog vw
		JOIN data_catalog.bronze_payload_control bpc
			ON bpc.tb_databases_id = vw.database_id
			AND bpc.tb_schemas_id = vw.schema_id
			AND bpc.tb_tables_id = vw.table_id
		WHERE vw.database_status_id = 5
		AND vw.schema_status_id = 5
		AND vw.table_status_id = 5
		AND vw.database_id = p_databases_id
		AND vw.schema_id = p_schemas_id
		AND vw.table_id = p_tables_id
	LOOP
		-- SE NÃO EXISTIR O VALOR DE TARGET_TIMESTAMP (INSERT_TIMESTAMP) NA TABELA
		-- É PORQUE TEM ALGUM ERRO AÍ.
		-- ENTÃO A GENTE RETORNA UMA QUERY NULA
		v_target_timestamp := v_record.target_timestamp;
		IF v_target_timestamp IS NULL THEN
			RETURN QUERY
				SELECT	null::VARCHAR(32) AS tb_databases_id
						, null::VARCHAR(32) AS tb_schemas_id
						, null::VARCHAR(32) AS tb_tables_id
						, null::TIMESTAMP AS insert_timestamp
						, null::BIGINT AS insert_qty
						, null::TIMESTAMP AS update_timestamp
						, null::BIGINT AS update_qty
						, null::TIMESTAMP AS delete_timestamp
						, null::BIGINT AS delete_qty
						, null::TIMESTAMP AS created_at
						, null::TIMESTAMP AS updated_at;
			RETURN;
		END IF;

		-- CRIAÇÃO DE UMA VARIÁVEL CONTENDO A DATA DO ÚLTIMO PROCESSAMENTO DE COMANDOS INSERTS
		-- PORÉM NÃO VAMOS USAR A DATA DE FORMA EXATA
		-- VAMOS CRIAR UMA MARGEM DE SEGURANÇA PARA PEGAR OS DADOS E FAZER UMA OPERAÇÃO DE UPSERT
		-- ESSA MARGEM DE SEGURANÇA SERÁ COM BASE NO TEMPO DO PERÍODO DE PAYLOAD DA TABELA
		-- POR EXEMPLO, SE A TABELA POSSUI O TB_PAYLOAD_PERIOD_ID A CADA 5 MINUTOS,
		-- DEVEREMOS PEGAR OS DADOS DA TABELA DE INSERTS A CADA 5 MINUTOS + 5% DESTE TEMPO ARREDONDADO PARA CIMA
		-- OU SEJA, AO INVÉS DE PEGAR OS REGISTROS A CADA 5 MINUTOS, PEGAREMOS A CADA 6 MINUTOS (5% DE 5 É 0,25 QUE ARREDONDADO PARA CIMA DÁ 1, OU SEJA, 5 + 1 = 6)
		v_minutes_safety_margin := CEILING((v_record.table_payload_period_minutes*0.05)::DOUBLE PRECISION);
		v_target_timestamp_safety_margin := v_target_timestamp - (v_minutes_safety_margin * INTERVAL '1 minutes');

		/*
			APÓS A CRIAÇÃO DAS VARIÁVEIS ABAIXO, QUE SERÃO USADAS COMO ATALHOS PARA A QUERY DINÂMCIA QUE SERÁ FORMA DENTRO DA VARIÁVEL V_CMD
			O RESULTADO DA QUERY DINÂMICA DEVERÁ SER SEMELHANTE À QUERY ABAIXO:
			
			WITH datasource AS (
				SELECT [table_name]_new->>'coluna1' AS "coluna1", [table_name]_new->>'coluna2' AS "coluna1", [table_name]_new->>'colunaN' AS "colunaN", executed_at
				FROM [database_name]_[schema_name]_raw_hstlog.[table_name]_hstlog_insert
				WHERE executed_at::TIMESTAMP(0) >= v_target_timestamp_safety_margin
				ORDER BY executed_at ASC
			)
			, new_target_timestamp AS (
				SELECT MAX(executed_at) AS executed_at
				FROM datasource
			)
			, op AS (
				INSERT INTO [database_name]_[schema_name].[table_name] ("coluna1","coluna2","colunaN")
				SELECT "coluna1","coluna2","colunaN"
				FROM datasource
				ON CONFLICT ON CONSTRAING [table_primary_key_name] DO NOTHING
				RETURNING [columns_pk]
			)
			SELECT ntt.executed_at, COUNT(op.*)
			FROM new_target_timestamp ntt, op
			GROUP BY ntt.executed_at
		*/

		-- BRONZE COLUMNS AND HSTLOG COLUMNS
		SELECT 	string_agg(CONCAT('"', column_name, '"'),', ') AS bronze_columns
				, string_agg(CONCAT('("',v_record.table_name,'_new"->>','''', column_name, ''')::', data_type, ' AS "', column_name, '"'),', ') AS hstlog_new_columns
		FROM (
			SELECT column_name, data_type
			FROM information_schema.columns
			WHERE table_schema = REPLACE(v_record.bronze_schema_name,'"','')
			AND table_name = v_record.table_name
			ORDER BY ordinal_position
		) columns
		INTO v_bronze_columns, v_hstlog_new_columns;

		-- PK COLUMNS FOR UPSERT
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

		-- PAYLOAD
		v_cmd := $cmd$
			WITH datasource AS (
				SELECT	$cmd$ || v_hstlog_new_columns || $cmd$
						, executed_at
				FROM $cmd$ || v_record.hstlog_insert_path || $cmd$
				WHERE executed_at::TIMESTAMP(0) >= '$cmd$ || v_target_timestamp_safety_margin || $cmd$'
				ORDER BY executed_at ASC
			), new_target_timestamp AS (
				SELECT MAX(executed_at) AS executed_at
				FROM datasource
			), op AS (
				INSERT INTO $cmd$ || v_record.bronze_path || $cmd$ ($cmd$ || v_bronze_columns || $cmd$)
				SELECT $cmd$ || v_bronze_columns || $cmd$
				FROM datasource
				ON CONFLICT ON CONSTRAINT "$cmd$ || v_bronze_columns_pk_name || $cmd$" DO NOTHING
				RETURNING $cmd$ || v_bronze_columns_pk || $cmd$
			)
			SELECT ntt.executed_at, COUNT(op.*)
			FROM new_target_timestamp ntt, op
			GROUP BY ntt.executed_at
		$cmd$;

		-- SE O PAYLOAD FOR CONCLUÍDO COM SUCESSO, ATUALIZAREMOS A TABELA DATA_CATALOG.BRONZE_PAYLOAD_CONTROL
		IF v_cmd IS NOT NULL THEN
			EXECUTE v_cmd INTO v_target_timestamp, v_qty;
			
			UPDATE data_catalog.bronze_payload_control SET
			insert_timestamp = COALESCE(v_target_timestamp,insert_timestamp),
			insert_qty = COALESCE(v_qty,0),
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id;

			RETURN QUERY
				SELECT	tb_databases_id::VARCHAR(32) AS tb_databases_id
						, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
						, tb_tables_id::VARCHAR(32) AS tb_tables_id
						, insert_timestamp::TIMESTAMP AS insert_timestamp
						, insert_qty::BIGINT AS insert_qty
						, update_timestamp::TIMESTAMP AS update_timestamp
						, update_qty::BIGINT AS update_qty
						, delete_timestamp::TIMESTAMP AS delete_timestamp
						, delete_qty::BIGINT AS delete_qty
						, created_at::TIMESTAMP AS created_at
						, updated_at::TIMESTAMP AS updated_at
				FROM data_catalog.bronze_payload_control
				WHERE tb_databases_id = v_record.database_id
				AND tb_schemas_id = v_record.schema_id
				AND tb_tables_id = v_record.table_id;
			RETURN;
		END IF;
	END LOOP;

	-- CASO TUDO DÊ ERRO, RETORNAMOS UMA QUERY NULA
	RETURN QUERY
		SELECT	null::VARCHAR(32) AS tb_databases_id
				, null::VARCHAR(32) AS tb_schemas_id
				, null::VARCHAR(32) AS tb_tables_id
				, null::TIMESTAMP AS insert_timestamp
				, null::BIGINT AS insert_qty
				, null::TIMESTAMP AS update_timestamp
				, null::BIGINT AS update_qty
				, null::TIMESTAMP AS delete_timestamp
				, null::BIGINT AS delete_qty
				, null::TIMESTAMP AS created_at
				, null::TIMESTAMP AS updated_at;
	RETURN;
	
END; 
$BODY$;

CREATE OR REPLACE FUNCTION data_catalog.bronze_payload(
	p_tb_status_id integer DEFAULT NULL::integer)
    RETURNS SETOF data_catalog.bronze_payload_control 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE v_record RECORD;
DECLARE v_bronze_payload_control RECORD;
DECLARE v_tb_status_id INTEGER DEFAULT 1; -- SE O PARÂMETRO NÃO FOR PASSADO, VAMOS EXECUTAR PARA O PRIMEIRO PERÍODO DE PAYLOAD (A CADA 5 MINUTOS)
BEGIN

	-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	-- Validação para evitar problemas na concorrência da chamada da função
	-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	IF NOT pg_try_advisory_lock(666666 + p_tb_status_id) THEN
	    RAISE NOTICE 'data_catalog.bronze_payload(%) já está em execução. Abortando.',p_tb_status_id;
		RETURN QUERY
			SELECT	null::VARCHAR(32) AS tb_databases_id
					, null::VARCHAR(32) AS tb_schemas_id
					, null::VARCHAR(32) AS tb_tables_id
					, null::TIMESTAMP AS insert_timestamp
					, null::BIGINT AS insert_qty
					, null::TIMESTAMP AS update_timestamp
					, null::BIGINT AS update_qty
					, null::TIMESTAMP AS delete_timestamp
					, null::BIGINT AS delete_qty
					, null::TIMESTAMP AS created_at
					, null::TIMESTAMP AS updated_at;
	    RETURN;
	END IF;

	-- SE O PARÂMETRO NÃO FOR PASSADO, VAMOS EXECUTAR PARA O PRIMEIRO PERÍODO DE PAYLOAD (A CADA 5 MINUTOS)
	v_tb_status_id := COALESCE(p_tb_status_id,v_tb_status_id);

	-- USANDO LOOP PARA NÃO PRECISAR CRIAR DIVERSAS VARIÁVEIS
	-- ASSIM CRIAMOS APENAS UMA VARIÁVEL DO TIPO RECORD COM TODOS OS DADOS QUE PRECISAMOS DE FORMA DINÂMICA
	FOR v_record IN
		SELECT	DISTINCT
				vw.database_id
				, vw.schema_id
				, vw.table_id
		FROM data_catalog.vw_catalog vw
		WHERE vw.database_status_id = 5
		AND vw.schema_status_id = 5
		AND vw.table_status_id = 5
		AND vw.table_payload_period_id = v_tb_status_id
	LOOP
		-- PAYLOAD COM INSERTS
		BEGIN
		SELECT	tb_databases_id::VARCHAR(32) AS tb_databases_id
				, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
				, tb_tables_id::VARCHAR(32) AS tb_tables_id
				, insert_timestamp::TIMESTAMP AS insert_timestamp
				, insert_qty::BIGINT AS insert_qty
				, update_timestamp::TIMESTAMP AS update_timestamp
				, update_qty::BIGINT AS update_qty
				, delete_timestamp::TIMESTAMP AS delete_timestamp
				, delete_qty::BIGINT AS delete_qty
				, created_at::TIMESTAMP AS created_at
				, updated_at::TIMESTAMP AS updated_at
		FROM data_catalog.bronze_payload_inserts(
			v_record.database_id,
			v_record.schema_id,
			v_record.table_id
		) INTO v_bronze_payload_control;
		EXCEPTION WHEN OTHERS THEN
			INSERT INTO data_catalog.bronze_payload_erros (
				tb_databases_id,
				tb_schemas_id,
				tb_tables_id,
				op,
				error_message,
				error_detail,
				created_at
			)
			VALUES (
				v_record.database_id,
				v_record.schema_id,
				v_record.table_id,
				'bronze_payload_inserts',
				SQLERRM,
				PG_EXCEPTION_DETAIL,
				clock_timestamp()
			);
		END;

		-- PAYLOAD COM UPDATES
		BEGIN
		SELECT	tb_databases_id::VARCHAR(32) AS tb_databases_id
				, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
				, tb_tables_id::VARCHAR(32) AS tb_tables_id
				, insert_timestamp::TIMESTAMP AS insert_timestamp
				, insert_qty::BIGINT AS insert_qty
				, update_timestamp::TIMESTAMP AS update_timestamp
				, update_qty::BIGINT AS update_qty
				, delete_timestamp::TIMESTAMP AS delete_timestamp
				, delete_qty::BIGINT AS delete_qty
				, created_at::TIMESTAMP AS created_at
				, updated_at::TIMESTAMP AS updated_at
		FROM data_catalog.bronze_payload_updates(
			v_record.database_id,
			v_record.schema_id,
			v_record.table_id
		) INTO v_bronze_payload_control;
		EXCEPTION WHEN OTHERS THEN
			INSERT INTO data_catalog.bronze_payload_erros (
				tb_databases_id,
				tb_schemas_id,
				tb_tables_id,
				op,
				error_message,
				error_detail,
				created_at
			)
			VALUES (
				v_record.database_id,
				v_record.schema_id,
				v_record.table_id,
				'bronze_payload_updates',
				SQLERRM,
				PG_EXCEPTION_DETAIL,
				clock_timestamp()
			);
		END;

		-- PAYLOAD COM DELETES
		BEGIN
		RETURN QUERY
			SELECT	tb_databases_id::VARCHAR(32) AS tb_databases_id
					, tb_schemas_id::VARCHAR(32) AS tb_schemas_id
					, tb_tables_id::VARCHAR(32) AS tb_tables_id
					, insert_timestamp::TIMESTAMP AS insert_timestamp
					, insert_qty::BIGINT AS insert_qty
					, update_timestamp::TIMESTAMP AS update_timestamp
					, update_qty::BIGINT AS update_qty
					, delete_timestamp::TIMESTAMP AS delete_timestamp
					, delete_qty::BIGINT AS delete_qty
					, created_at::TIMESTAMP AS created_at
					, updated_at::TIMESTAMP AS updated_at
			FROM data_catalog.bronze_payload_deletes(
				v_record.database_id,
				v_record.schema_id,
				v_record.table_id
			);
		EXCEPTION WHEN OTHERS THEN
			INSERT INTO data_catalog.bronze_payload_erros (
				tb_databases_id,
				tb_schemas_id,
				tb_tables_id,
				op,
				error_message,
				error_detail,
				created_at
			)
			VALUES (
				v_record.database_id,
				v_record.schema_id,
				v_record.table_id,
				'bronze_payload_deletes',
				SQLERRM,
				PG_EXCEPTION_DETAIL,
				clock_timestamp()
			);
		END;
			
	END LOOP;

	RETURN;
END; 
$BODY$;
