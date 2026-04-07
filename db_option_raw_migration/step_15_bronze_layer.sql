CREATE OR REPLACE FUNCTION data_catalog.bronze_layer(
	p_status_object integer)
    RETURNS TABLE(object_type character varying, object_name character varying, object_status_from character varying, object_status_to character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE v_record RECORD;
DECLARE v_record2 RECORD;
DECLARE v_record3 RECORD;
DECLARE v_cmd TEXT;
DECLARE v_status_from VARCHAR;
DECLARE v_status_to VARCHAR;
DECLARE v_object_name VARCHAR(250);
DECLARE v_object_description TEXT;
DECLARE v_columns_pk VARCHAR;
DECLARE v_columns_pk_check INTEGER;
DECLARE v_table_has_pk BOOLEAN DEFAULT FALSE;
BEGIN
/*
    Função: data_catalog.bronze_layer
    Objetivo:
        Gerenciar o ciclo de vida de objetos (databases, schemas, tables, columns)
        na camada Bronze do Data Platform, com base nos status definidos no catálogo.

    Escopo:
        - Criar objetos aprovados para criação.
        - Remover objetos aprovados para remoção.
        - Atualizar status intermediários para controle de paralelismo e segurança.
        - Registrar transições de status no catálogo.

    Status relevantes:
        2 -> 4 : Preparação para criação (somente mudança de status)
        4 -> 5 : Criação dos objetos + mudança de status
        6 -> 7 : Preparação para remoção (somente mudança de status)
        7 -> 8 : Remoção dos objetos + mudança de status

    Observações:
        - A função deve ser idempotente por status.
        - A criação ocorre sempre a partir do schema RAW.
        - A remoção ocorre sempre na camada Bronze.
*/

-- --------------------------------------------------------------------------------------------------------------------------------------------------------
-- Validação para evitar problemas na concorrência da chamada da função
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
IF NOT pg_try_advisory_lock(999999 + p_status_object) THEN
    RAISE NOTICE 'data_catalog.bronze_layer(%) já está em execução. Abortando.', p_status_object;
	RETURN QUERY
		SELECT	null AS object_type
				, null AS object_name
				, null AS object_status_from
				, null AS object_status_to;
    RETURN;
END IF;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------
-- Determina o status de origem e destino com base no status atual do objeto. -- Cada bloco representa uma transição válida no fluxo de governança.
-- Bloco da função para argumento p_status_object = 2
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	IF p_status_object = 2 THEN
	-- 2 -> 4 : Objeto aprovado para criação, movido para fila de criação.
	
		SELECT "name" FROM data_catalog.tb_status WHERE id = 2 INTO v_status_from;
		SELECT "name" FROM data_catalog.tb_status WHERE id = 4 INTO v_status_to;
		
		-- ==================================================
		-- DATABASES
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_databases que foram aprovados para camada bronze (status 2)
		-- O novo status do objeto deverá ser 4 para entrar no motor de criação de objetos
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, database_description
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id = 2
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id = 2
		LOOP
			UPDATE data_catalog.tb_databases SET 
			tb_status_id = 4,
			updated_at = clock_timestamp()
			WHERE id = v_record.database_id;
			RETURN QUERY
				SELECT	DISTINCT
						'TO CREATE DATABASE'::VARCHAR AS object_type
						, v_record.database_name AS object_name
						, v_status_from AS object_status_from
						, v_status_to AS object_status_to;
		END LOOP;
		-- ==================================================
		-- SCHEMAS
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_schemas que foram aprovados para camada bronze (status 2)
		-- O novo status do objeto deverá ser 4 para entrar no motor de criação de objetos
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, database_description
					, schema_id
					, schema_name
					, schema_description
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (2,4,5)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id = 2
		LOOP
			UPDATE data_catalog.tb_schemas SET 
			tb_status_id = 4,
			updated_at = clock_timestamp()
			WHERE id = v_record.schema_id
			AND tb_databases_id = v_record.database_id;
			RETURN QUERY
				SELECT	DISTINCT
						'TO CREATE SCHEMA'::VARCHAR AS object_type
						, CONCAT(v_record.database_name,'.',v_record.schema_name)::VARCHAR AS object_name
						, v_status_from AS object_status_from
						, v_status_to AS object_status_to;
		END LOOP;
		-- ==================================================
		-- TABLES
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_tables que foram aprovados para camada bronze (status 2)
		-- O novo status do objeto deverá ser 4 para entrar no motor de criação de objetos
		--
		-- IMPORTANTE: a tabela só será aprovada se ela possuir uma de suas colunas como sendo PK
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					vw.database_id
					, vw.database_name
					, vw.schema_id
					, vw.schema_name
					, vw.table_id
					, vw.table_name
					, vw.table_description
					, (CASE WHEN vw2.table_id IS NOT NULL THEN TRUE ELSE FALSE END) AS table_has_pk
			FROM data_catalog.vw_catalog vw
			JOIN LATERAL (
				SELECT DISTINCT vw2.database_id, vw2.schema_id, vw2.table_id
				FROM data_catalog.vw_catalog vw2
				WHERE vw2.database_id = vw.database_id
				AND vw2.schema_id = vw.schema_id
				AND vw2.table_id = vw.table_id
				AND vw2.column_pk IS TRUE
			) vw2 
				ON vw2.database_id = vw.database_id
				AND vw2.schema_id = vw.schema_id
				AND vw2.table_id = vw.table_id
			WHERE vw.database_active IS TRUE
			AND vw.database_description IS NOT NULL
			AND vw.database_status_id IN (2,4,5)
			AND vw.schema_active IS TRUE
			AND vw.schema_description IS NOT NULL
			AND vw.schema_status_id IN (2,4,5)
			AND vw.table_active IS TRUE
			AND vw.table_description IS NOT NULL
			AND vw.table_payload_period_id IS NOT NULL
			AND vw.table_status_id IN (2,4,5)
			AND vw.column_active IS TRUE
			AND vw.column_description IS NOT NULL
			AND vw.column_data_type IS NOT NULL
			AND vw.column_status_id = 2
		LOOP
			-- Depositamos nesta variável a condição da tabela obrigatoriamente possuir uma primary key
			-- Com isso, liberamos ou não a criação de colunas na tabela.
			v_table_has_pk := v_record.table_has_pk;
			
			UPDATE data_catalog.tb_tables SET 
			tb_status_id = 4,
			updated_at = clock_timestamp()
			WHERE id = v_record.table_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_databases_id = v_record.database_id;
			RETURN QUERY
				SELECT	DISTINCT
						'TO CREATE TABLE'::VARCHAR AS object_type
						, CONCAT(v_record.database_name,'.',v_record.schema_name,'.',v_record.table_name)::VARCHAR AS object_name
						, v_status_from AS object_status_from
						, v_status_to AS object_status_to;
		END LOOP;
		-- ==================================================
		-- COLUMNS
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_columns que foram aprovados para camada bronze (status 2)
		-- O novo status do objeto deverá ser 4 para entrar no motor de criação de objetos
		--
		-- Antes de iniciar, validamos se a tabela possui uma PK
		-- ==================================================
		IF v_table_has_pk THEN
			FOR v_record IN
				SELECT	DISTINCT
						database_id
						, database_name
						, schema_id
						, schema_name
						, table_id
						, table_name
						, column_id
						, column_name
						, column_description
				FROM data_catalog.vw_catalog
				WHERE database_active IS TRUE
				AND database_description IS NOT NULL
				AND database_status_id IN (2,4,5)
				AND schema_active IS TRUE
				AND schema_description IS NOT NULL
				AND schema_status_id IN (2,4,5)
				AND table_active IS TRUE
				AND table_description IS NOT NULL
				AND table_payload_period_id IS NOT NULL
				AND table_status_id IN (2,4,5)
				AND column_active IS TRUE
				AND column_description IS NOT NULL
				AND column_data_type IS NOT NULL
				AND column_status_id = 2
			LOOP
				UPDATE data_catalog.tb_columns SET 
				tb_status_id = 4,
				updated_at = clock_timestamp()
				WHERE id = v_record.column_id
				AND tb_tables_id = v_record.table_id
				AND tb_schemas_id = v_record.schema_id
				AND tb_databases_id = v_record.database_id;
				RETURN QUERY
					SELECT	DISTINCT
							'TO CREATE COLUMN'::VARCHAR AS object_type
							, CONCAT(v_record.database_name,'.',v_record.schema_name,'.',v_record.table_name,'.',v_record.column_name)::VARCHAR AS object_name
							, v_status_from AS object_status_from
							, v_status_to AS object_status_to;
			END LOOP;
		END IF;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------
-- Determina o status de origem e destino com base no status atual do objeto. -- Cada bloco representa uma transição válida no fluxo de governança.
-- Bloco da função para argumento p_status_object = 4
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	ELSIF p_status_object = 4 THEN
	-- ----------------------------------------------------------------
	-- 4 -> 5 : Execução da criação física do objeto na camada Bronze.
	
		SELECT "name" FROM data_catalog.tb_status WHERE id = 4 INTO v_status_from;
		SELECT "name" FROM data_catalog.tb_status WHERE id = 5 INTO v_status_to;

		-- ==================================================
		-- DATABASES + SCHEMAS : ETAPA DE CRIAÇÃO E DOCUMENTAÇÃO/COMMENT DE NOVOS SCHEMAS NA CAMADA BRONZE
		-- 
		-- Este loop é responsável por gerar um código SQL dinâmico para criação dos novos schemas na camada bronze.
		-- O padrão de nomenclatura dos objetos deverá seguir ao que já estamos acostumados: [banco_de_dados]_[schema]
		-- Exemplo: qd_user_db_public / qd_user_db_augym
		-- Após a criação do novo schema, o novo status do objeto deverá ser 5 para informar sua disponibilidade na camada bronze
		--
		-- Regras e restrições:
		--    - tb_databases.tb_status_id IN (4,5)
		--    - tb_databases.description não pode ser NULL
		--    - tb_databases.active precisa ser TRUE
		--    - tb_schemas.tb_status_id = 4
		--    - tb_schemas.description não pode ser NULL
		--    - tb_schemas.active precisa ser TRUE
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, database_description
					, schema_id
					, schema_name
					, schema_description
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (4,5)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id = 4
		LOOP
			-- Criação do padrão da nomenclatura. Se for necessário alterar o padrão, basta ajustar aqui.
			v_object_name := CONCAT(v_record.database_name,'_',v_record.schema_name);
			-- Separação da documentação/comment do objeto. O padrão será DATABASE = [nome do banco de dados] ([documentação do database]) | SCHEMA = [nome do schema] ([documentação do schema])
			v_object_description := 'DATABASE = ' || v_record.database_name || ' (' || v_record.database_description || ') | SCHEMA = ' || v_record.schema_name || ' (' || v_record.schema_description || ')';

			-- Técnica de dólar quoting, onde usamos o $$ como se fosse uma string
			-- Por exemplo: 'Data Platform' = $$Data Platform$$ = $qualquercoisa$$Data Platform$qualquercoisa$
			v_cmd := $cmd$
				CREATE SCHEMA IF NOT EXISTS "$cmd$ || v_object_name || $cmd$";
				COMMENT ON SCHEMA "$cmd$ || v_object_name || $cmd$" IS '$cmd$ || v_object_description || $cmd$';
			$cmd$;

			-- Validamos se a variável contendo o SQL dinâmico está em branco.
			-- Se não estiver, executamos a string da variável criando o objeto e alterando o tb_status_id dele para o status correto
			-- Retornamos também uma parte do retorno informando qual schema foi criado
			IF v_cmd IS NOT NULL THEN
				EXECUTE v_cmd;
				UPDATE data_catalog.tb_databases SET 
				tb_status_id = 5,
				updated_at = clock_timestamp()
				WHERE id = v_record.database_id;
				UPDATE data_catalog.tb_schemas SET 
				tb_status_id = 5,
				updated_at = clock_timestamp()
				WHERE id = v_record.schema_id
				AND tb_databases_id = v_record.database_id;
	
				RETURN QUERY
					SELECT	DISTINCT
							'CREATE SCHEMA'::VARCHAR AS object_type
							, CONCAT(v_record.database_name,'_',v_record.schema_name)::VARCHAR AS object_name
							, v_status_from AS object_status_from
							, v_status_to AS object_status_to;
			END IF;
		END LOOP;

		-- ==================================================
		-- TABELAS + COLUNAS : ETAPA DE CRIAÇÃO E DOCUMENTAÇÃO/COMMENT DE NOVAS TABELAS COM SUAS DEVIDAS COLUNAS
		-- 
		-- Este loop é responsável por gerar um código SQL dinâmico para criação das novas tabelas na camada bronze.
		-- Após a criação da nova tabela com suas respectivas colunas, o novo status do objeto deverá ser 5 para informar sua disponibilidade na camada bronze
		--
		-- Regras e restrições:
		--    - tb_databases.tb_status_id IN (4,5)
		--    - tb_databases.description não pode ser NULL
		--    - tb_databases.active precisa ser TRUE
		--    - tb_schemas.tb_status_id IN (4,5)
		--    - tb_schemas.description não pode ser NULL
		--    - tb_schemas.active precisa ser TRUE
		--    - tb_tables.tb_status_id = 4
		--    - tb_tables.description não pode ser NULL
		--    - tb_tables.tb_payload_period_id não pode ser NULL (indica qual a periodicidade que deveremos fazer a carga dos dados nessa tabela)
		--    - tb_tables.active precisa ser TRUE
		--    - tb_columns.tb_status_id = 4
		--    - tb_columns.description não pode ser NULL
		--    - tb_columns.data_type não pode ser NULL
		--    - tb_columns.active precisa ser TRUE
		-- ==================================================
		-- RAISE EXCEPTION 'ERRO NA CRIAÇÃO DA PK';
		FOR v_record IN
			SELECT	vw.database_id
					, vw.database_name
					, vw.schema_id
					, vw.schema_name
					, vw.table_id
					, vw.table_name
					, vw.table_description
					, (CASE WHEN vw2.table_id IS NOT NULL THEN TRUE ELSE FALSE END) AS table_has_pk
					, string_agg(CONCAT('"',vw.column_name,'" ') || vw.column_data_type,', ') AS table_columns
			FROM data_catalog.vw_catalog vw
			JOIN LATERAL (
				SELECT DISTINCT vw2.database_id, vw2.schema_id, vw2.table_id
				FROM data_catalog.vw_catalog vw2
				WHERE vw2.database_id = vw.database_id
				AND vw2.schema_id = vw.schema_id
				AND vw2.table_id = vw.table_id
				AND vw2.column_pk IS TRUE
			) vw2 
				ON vw2.database_id = vw.database_id
				AND vw2.schema_id = vw.schema_id
				AND vw2.table_id = vw.table_id
			WHERE vw.database_active IS TRUE
			AND vw.database_description IS NOT NULL
			AND vw.database_status_id IN (4,5)
			AND vw.schema_active IS TRUE
			AND vw.schema_description IS NOT NULL
			AND vw.schema_status_id IN (4,5)
			AND vw.table_active IS TRUE
			AND vw.table_description IS NOT NULL
			AND vw.table_payload_period_id IS NOT NULL
			AND vw.table_status_id = 4
			AND vw.column_active IS TRUE
			AND vw.column_description IS NOT NULL
			AND vw.column_data_type IS NOT NULL
			AND vw.column_status_id = 4
			GROUP BY 1,2,3,4,5,6,7,8
		LOOP
			-- Depositamos nesta variável a condição da tabela obrigatoriamente possuir uma primary key
			-- Com isso, liberamos ou não a criação de colunas na tabela.
			v_table_has_pk := v_record.table_has_pk;
			
			-- Concatenação para formar o nome do novo schema existente na camada bronze
			v_object_name := CONCAT(v_record.database_name,'_',v_record.schema_name);
			
			-- Nesta query abaixo, vamos pegar todas as colunas da tabela que está sendo criada e que estão definidas como PK
			-- Deixei em uma query separada para caso tenhamos tabelas com mais de 1 coluna sendo PK.
			SELECT string_agg(CONCAT('"', c.name, '"'),',') AS columns_pk
			FROM data_catalog.tb_columns c
			WHERE c.tb_databases_id = v_record.database_id
			AND c.tb_schemas_id = v_record.schema_id
			AND c.tb_tables_id = v_record.table_id
			AND c.is_pk IS TRUE
			INTO v_columns_pk;
			
			-- Técnica de dólar quoting, onde usamos o $$ como se fosse uma string
			-- Por exemplo: 'Data Platform' = $$Data Platform$$ = $qualquercoisa$$Data Platform$qualquercoisa$
			v_cmd := $cmd$
				CREATE TABLE IF NOT EXISTS "$cmd$ || v_object_name || $cmd$"."$cmd$ || v_record.table_name || $cmd$" ($cmd$ || v_record.table_columns || $cmd$);
				COMMENT ON TABLE "$cmd$ || v_object_name || $cmd$"."$cmd$ || v_record.table_name || $cmd$" IS '$cmd$ || v_record.table_description || $cmd$';
			$cmd$;

			SELECT COUNT(tc.*)
			FROM information_schema.table_constraints tc
			WHERE tc.table_schema = v_object_name
			AND tc.table_name = v_record.table_name
			AND tc.constraint_type = 'PRIMARY KEY'
			INTO v_columns_pk_check;

			-- Se retornar campos que são PK da tabela e que foram salvos na variável v_columns_pk,
			-- então vamos criar o comando para adicionar a constraint de primary key à tabela nova.
			-- Neste ponto, eu vou concatenar o que já existe na variável v_cmd com mais um dólar quoting.
			-- Também é validado se a PK já existe na tabela com base na query de information_schema.table_constraints
			IF v_columns_pk IS NOT NULL AND v_columns_pk_check = 0 THEN
				v_cmd := v_cmd || $cmd$
					ALTER TABLE IF EXISTS "$cmd$ || v_object_name || $cmd$"."$cmd$ || v_record.table_name || $cmd$"
					ADD CONSTRAINT "$cmd$ || v_record.table_name || $cmd$_pk" PRIMARY KEY ($cmd$ || v_columns_pk || $cmd$);
				$cmd$;
			END IF;
			
			IF v_cmd IS NOT NULL THEN
				EXECUTE v_cmd;
					UPDATE data_catalog.tb_tables SET 
					tb_status_id = 5,
					updated_at = clock_timestamp()
					WHERE id = v_record.table_id
					AND tb_databases_id = v_record.database_id
					AND tb_schemas_id = v_record.schema_id;
		
					RETURN QUERY
						SELECT	DISTINCT
								'CREATE TABLE'::VARCHAR AS object_type
								, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name)::VARCHAR AS object_name
								, v_status_from AS object_status_from
								, v_status_to AS object_status_to;
			END IF;

			-- Este loop auxiliar serve para documentarmos/comment as colunas que recém foram criadas
			FOR v_record2 IN
				SELECT	DISTINCT
						database_id
						, database_name
						, schema_id
						, schema_name
						, table_id
						, table_name
						, column_id
						, column_name
						, column_description
				FROM data_catalog.vw_catalog
				WHERE database_id = v_record.database_id
				AND schema_id = v_record.schema_id
				AND table_id = v_record.table_id
				AND column_active IS TRUE
				AND column_description IS NOT NULL
				AND column_data_type IS NOT NULL
				AND column_status_id = 4
			LOOP
				-- Técnica de dólar quoting, onde usamos o $$ como se fosse uma string
				-- Por exemplo: 'Data Platform' = $$Data Platform$$ = $qualquercoisa$$Data Platform$qualquercoisa$
				v_cmd := $cmd$
					COMMENT ON COLUMN "$cmd$ || v_object_name || $cmd$"."$cmd$ || v_record2.table_name || $cmd$"."$cmd$ || v_record2.column_name || $cmd$" IS '$cmd$ || v_record2.column_description || $cmd$';
				$cmd$;
	
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
					UPDATE data_catalog.tb_columns SET 
					tb_status_id = 5,
					updated_at = clock_timestamp()
					WHERE id = v_record2.column_id
					AND tb_databases_id = v_record2.database_id
					AND tb_schemas_id = v_record2.schema_id
					AND tb_tables_id = v_record2.table_id;
		
					RETURN QUERY
						SELECT	DISTINCT
								'CREATE COLUMN'::VARCHAR AS object_type
								, CONCAT(v_record2.database_name,'_',v_record2.schema_name,'.',v_record2.table_name,'.',v_record2.column_name)::VARCHAR AS object_name
								, v_status_from AS object_status_from
								, v_status_to AS object_status_to;
				END IF;
			END LOOP;
		END LOOP;

		-- ==================================================
		-- TABELAS + COLUNAS : ETAPA DE ADIÇÃO E DOCUMENTAÇÃO/COMMENT DE NOVAS COLUNAS EM TABELAS JÁ EXISTENTES NA CAMADA BRONZE
		-- 
		-- Este loop é responsável por gerar um código SQL dinâmico para criação de novas colunas nas tabelas já existentes na camada bronze.
		-- Após a criação das novas colunas, o novo status do objeto deverá ser 5 para informar sua disponibilidade na camada bronze
		--
		-- Regras e restrições:
		--    - tb_databases.tb_status_id = 5
		--    - tb_databases.description não pode ser NULL
		--    - tb_databases.active precisa ser TRUE
		--    - tb_schemas.tb_status_id = 5
		--    - tb_schemas.description não pode ser NULL
		--    - tb_schemas.active precisa ser TRUE
		--    - tb_tables.tb_status_id = 5
		--    - tb_tables.description não pode ser NULL
		--    - tb_tables.tb_payload_period_id não pode ser NULL (indica qual a periodicidade que deveremos fazer a carga dos dados nessa tabela)
		--    - tb_tables.active precisa ser TRUE
		--    - tb_columns.tb_status_id = 4
		--    - tb_columns.description não pode ser NULL
		--    - tb_columns.data_type não pode ser NULL
		--    - tb_columns.active precisa ser TRUE
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					vw.database_id
					, vw.database_name
					, vw.schema_id
					, vw.schema_name
					, vw.table_id
					, vw.table_name
					, vw.column_id
					, vw.column_name
					, vw.column_data_type
					, vw.column_description
			FROM data_catalog.vw_catalog vw
			JOIN LATERAL (
				SELECT DISTINCT vw2.database_id, vw2.schema_id, vw2.table_id
				FROM data_catalog.vw_catalog vw2
				WHERE vw2.database_id = vw.database_id
				AND vw2.schema_id = vw.schema_id
				AND vw2.table_id = vw.table_id
				AND vw2.column_pk IS TRUE
			) vw2 
				ON vw2.database_id = vw.database_id
				AND vw2.schema_id = vw.schema_id
				AND vw2.table_id = vw.table_id
			WHERE vw.database_active IS TRUE
			AND vw.database_description IS NOT NULL
			AND vw.database_status_id = 5
			AND vw.schema_active IS TRUE
			AND vw.schema_description IS NOT NULL
			AND vw.schema_status_id = 5
			AND vw.table_active IS TRUE
			AND vw.table_description IS NOT NULL
			AND vw.table_payload_period_id IS NOT NULL
			AND vw.table_status_id = 5
			AND vw.column_active IS TRUE
			AND vw.column_description IS NOT NULL
			AND vw.column_data_type IS NOT NULL
			AND vw.column_status_id = 4
		LOOP
			-- Concatenação para formar o nome do novo schema existente na camada bronze
			v_object_name := CONCAT(v_record.database_name,'_',v_record.schema_name);
		
			-- Técnica de dólar quoting, onde usamos o $$ como se fosse uma string
			-- Por exemplo: 'Data Platform' = $$Data Platform$$ = $qualquercoisa$$Data Platform$qualquercoisa$
			v_cmd := $cmd$
				ALTER TABLE IF EXISTS "$cmd$ || v_object_name || $cmd$"."$cmd$ || v_record.table_name || $cmd$" ADD COLUMN IF NOT EXISTS "$cmd$ || v_record.column_name || $cmd$" $cmd$ || v_record.column_data_type || $cmd$;
				COMMENT ON COLUMN "$cmd$ || v_object_name || $cmd$"."$cmd$ || v_record.table_name || $cmd$"."$cmd$ || v_record.column_name || $cmd$" IS '$cmd$ || v_record.column_description || $cmd$';
			$cmd$;

			IF v_cmd IS NOT NULL THEN
				EXECUTE v_cmd;
				UPDATE data_catalog.tb_columns SET 
				tb_status_id = 5,
				updated_at = clock_timestamp()
				WHERE id = v_record.column_id
				AND tb_databases_id = v_record.database_id
				AND tb_schemas_id = v_record.schema_id
				AND tb_tables_id = v_record.table_id;
	
				RETURN QUERY
					SELECT	DISTINCT
							'CREATE COLUMN'::VARCHAR AS object_type
							, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name,'.',v_record.column_name)::VARCHAR AS object_name
							, v_status_from AS object_status_from
							, v_status_to AS object_status_to;
			END IF;
		END LOOP;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------
-- Determina o status de origem e destino com base no status atual do objeto. -- Cada bloco representa uma transição válida no fluxo de governança.
-- Bloco da função para argumento p_status_object = 6
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	ELSIF p_status_object = 6 THEN
	-- ----------------------------------------------------------------
	-- 6 -> 7 : Objeto aprovado para remoção, movido para fila de drop.
	
		SELECT "name" FROM data_catalog.tb_status WHERE id = 6 INTO v_status_from;
		SELECT "name" FROM data_catalog.tb_status WHERE id = 7 INTO v_status_to;

		-- ==================================================
		-- DATABASES
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_databases que foram aprovados para serem removidos da camada bronze (status 7)
		-- O novo status do objeto deverá ser 7 para entrar no motor de remoção de objetos
		-- Esta ação excluirá todos os objetos dependentes na hirearquia.
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, schema_id
					, schema_name
					, table_id
					, table_name
					, column_id
					, column_name
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id = 6
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id IN (5,6)
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id IN (5,6)
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id IN (5,6)
		LOOP
			UPDATE data_catalog.tb_databases SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE id = v_record.database_id
			AND tb_status_id = 6;
			
			UPDATE data_catalog.tb_schemas SET
			tb_status_id = 6,
			updated_at = clock_timestamp()
			WHERE id = v_record.schema_id
			AND tb_databases_id = v_record.database_id
			AND tb_status_id IN (5);

			UPDATE data_catalog.tb_schemas SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE id = v_record.schema_id
			AND tb_databases_id = v_record.database_id
			AND tb_status_id IN (6);
			
			UPDATE data_catalog.tb_tables SET
			tb_status_id = 6,
			updated_at = clock_timestamp()
			WHERE id = v_record.table_id
			AND tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_status_id IN (5);

			UPDATE data_catalog.tb_tables SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE id = v_record.table_id
			AND tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_status_id IN (6);

			UPDATE data_catalog.tb_columns SET
			tb_status_id = 6,
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id
			AND id = v_record.column_id
			AND tb_status_id IN (5);

			UPDATE data_catalog.tb_columns SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id
			AND id = v_record.column_id
			AND tb_status_id IN (6);

			RETURN QUERY
				SELECT	DISTINCT
						'TO DROP DATABASE + SCHEMA + TABLE + COLUMN'::VARCHAR AS object_type
						, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name,'.',v_record.column_name)::VARCHAR AS object_name
						, v_status_from AS object_status_from
						, v_status_to AS object_status_to;
		END LOOP;
		-- ==================================================
		-- SCHEMAS
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_schemas que foram aprovados para serem removidos da camada bronze (status 7)
		-- O novo status do objeto deverá ser 7 para entrar no motor de remoção de objetos
		-- Esta ação excluirá todos os objetos dependentes na hirearquia.
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, schema_id
					, schema_name
					, table_id
					, table_name
					, column_id
					, column_name
					, column_description
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (5,6)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id = 6
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id IN (5,6)
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id IN (5,6)
		LOOP
			UPDATE data_catalog.tb_schemas SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE id = v_record.schema_id
			AND tb_databases_id = v_record.database_id
			AND tb_status_id = 6;
			
			UPDATE data_catalog.tb_tables SET
			tb_status_id = 6,
			updated_at = clock_timestamp()
			WHERE id = v_record.table_id
			AND tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_status_id IN (5,6);

			UPDATE data_catalog.tb_tables SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE id = v_record.table_id
			AND tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_status_id IN (5,6);

			UPDATE data_catalog.tb_columns SET
			tb_status_id = 6,
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id
			AND id = v_record.column_id
			AND tb_status_id IN (5,6);

			UPDATE data_catalog.tb_columns SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id
			AND id = v_record.column_id
			AND tb_status_id IN (5,6);

			RETURN QUERY
				SELECT	DISTINCT
						'TO DROP SCHEMA + TABLE + COLUMN'::VARCHAR AS object_type
						, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name,'.',v_record.column_name)::VARCHAR AS object_name
						, v_status_from AS object_status_from
						, v_status_to AS object_status_to;
		END LOOP;
		-- ==================================================
		-- TABLES
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_tables que foram aprovados para serem removidos da camada bronze (status 7)
		-- O novo status do objeto deverá ser 7 para entrar no motor de remoção de objetos
		-- Esta ação excluirá todos os objetos dependentes na hirearquia.
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, schema_id
					, schema_name
					, table_id
					, table_name
					, table_status_id
					, column_id
					, column_name
					, column_status_id
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (5,6)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id IN (5,6)
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id = 6
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id IN (5,6)
		LOOP
			UPDATE data_catalog.tb_tables SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE id = v_record.table_id
			AND tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_status_id = 6;

			UPDATE data_catalog.tb_columns SET
			tb_status_id = 6,
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id
			AND id = v_record.column_id
			AND tb_status_id IN (5);

			UPDATE data_catalog.tb_columns SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id
			AND id = v_record.column_id
			AND tb_status_id IN (5,6);

			RETURN QUERY
				SELECT	DISTINCT
						'TO DROP TABLE + COLUMN'::VARCHAR AS object_type
						, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name,'.',v_record.column_name)::VARCHAR AS object_name
						, v_status_from AS object_status_from
						, v_status_to AS object_status_to;
		END LOOP;
		-- ==================================================
		-- COLUMNS
		-- 
		-- Este loop é responsável por atualizar o tb_status_id dos tb_columns que foram aprovados para serem removidos da camada bronze (status 7)
		-- O novo status do objeto deverá ser 7 para entrar no motor de remoção de objetos
		-- Esta ação excluirá todos os objetos dependentes na hirearquia.
		-- ==================================================
		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, schema_id
					, schema_name
					, table_id
					, table_name
					, column_id
					, column_name
					, column_description
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (5,6)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id IN (5,6)
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id IN (5,6)
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id = 6
		LOOP
			UPDATE data_catalog.tb_columns SET
			tb_status_id = 7,
			updated_at = clock_timestamp()
			WHERE tb_databases_id = v_record.database_id
			AND tb_schemas_id = v_record.schema_id
			AND tb_tables_id = v_record.table_id
			AND id = v_record.column_id
			AND tb_status_id = 6;

			RETURN QUERY
				SELECT	DISTINCT
						'TO DROP COLUMN'::VARCHAR AS object_type
						, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name,'.',v_record.column_name)::VARCHAR AS object_name
						, v_status_from AS object_status_from
						, v_status_to AS object_status_to;
		END LOOP;
		
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
-- Determina o status de origem e destino com base no status atual do objeto. -- Cada bloco representa uma transição válida no fluxo de governança.
-- Bloco da função para argumento p_status_object = 7
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	ELSIF p_status_object = 7 THEN
	-- ----------------------------------------------------------------
	-- 7 -> 8 : Execução da remoção física do objeto na camada Bronze.

		SELECT "name" FROM data_catalog.tb_status WHERE id = 7 INTO v_status_from;
		SELECT "name" FROM data_catalog.tb_status WHERE id = 8 INTO v_status_to;

		-- ==================================================
		-- DATABASES
		-- 
		-- Este loop é responsável por DROPAR toda a cadeia de objetos que está por baixo da hierarquia de tb_databases com tb_status_id = 7 (EM REMOÇÃO DATA PLATFORM)
		-- Para garantir segurança e respeitar principalmente tabelas de auditoria (localizadas em _hstlog), o DROP não poderá ser CASCADE.
		-- Teremos que efetuar o DROP subindo a escala de hierarquias (tb_tables + tb_columns -> tb_databases + tb_schemas)
		-- O novo status do objeto deverá ser 8 (REMOVIDO DATA PLATFORM)
		-- Esta ação excluirá todos os objetos dependentes na hirearquia.
		-- ==================================================

		FOR v_record IN
			SELECT	database_id
					, database_name
					, array_agg(DISTINCT schema_id) AS schemas_id
					, array_agg(DISTINCT schema_name) AS schemas_name
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (7)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id IN (5,6,7)
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id IN (5,6,7)
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id  IN (5,6,7)
			GROUP BY 1,2
		LOOP
			FOR v_record2 IN
				SELECT UNNEST(v_record.schemas_id) AS schema_id
			LOOP
				FOR v_record2 IN
					SELECT	database_id
							, database_name
							, schema_id
							, schema_name
							, array_agg(DISTINCT table_id) AS tables_id
							, array_agg(DISTINCT table_name) AS tables_name
					FROM data_catalog.vw_catalog
					WHERE database_id = v_record.database_id
					AND schema_id = v_record2.schema_id
					AND table_active IS TRUE
					AND table_description IS NOT NULL
					AND table_payload_period_id IS NOT NULL
					AND table_status_id IN (5,6,7)
					GROUP BY 1,2,3,4
				LOOP
					FOR v_record3 IN
						SELECT UNNEST(v_record2.tables_name) AS table_name
					LOOP
						v_cmd := $cmd$
							DROP TABLE IF EXISTS "$cmd$ || CONCAT(v_record.database_name,'_',v_record2.schema_name) || $cmd$"."$cmd$ || v_record3.table_name || $cmd$" CASCADE;
						$cmd$;
			
						IF v_cmd IS NOT NULL THEN
							EXECUTE v_cmd;
			
							UPDATE data_catalog.tb_tables SET
							tb_status_id = 8,
							updated_at = clock_timestamp()
							WHERE tb_databases_id = v_record.database_id
							AND tb_schemas_id = v_record2.schema_id
							AND id = ANY(v_record2.tables_id)
							AND tb_status_id IN (5,6,7);
							
							UPDATE data_catalog.tb_columns SET
							tb_status_id = 8,
							updated_at = clock_timestamp()
							WHERE tb_databases_id = v_record.database_id
							AND tb_schemas_id = v_record2.schema_id
							AND tb_tables_id = ANY(v_record2.tables_id)
							AND tb_status_id IN (5,6,7);
							
							RETURN QUERY
								SELECT	DISTINCT
										'DROP TABLE'::VARCHAR AS object_type
										, CONCAT(v_record.database_name,'_',v_record2.schema_name,'.',v_record3.table_name)::VARCHAR AS object_name
										, v_status_from AS object_status_from
										, v_status_to AS object_status_to;
						END IF;
					END LOOP;
		
					IF CONCAT(v_record.database_name,'_',v_record2.schema_name) NOT IN ('public','information_schema','data_catalog') THEN
						v_cmd := $cmd$
							DROP SCHEMA "$cmd$ || CONCAT(v_record.database_name,'_',v_record2.schema_name) || $cmd$";
						$cmd$;
			
						IF v_cmd IS NOT NULL THEN
							EXECUTE v_cmd;

							UPDATE data_catalog.tb_databases SET
							tb_status_id = 8,
							updated_at = clock_timestamp()
							WHERE id = v_record.database_id
							AND tb_status_id = 7;
							
							UPDATE data_catalog.tb_schemas SET
							tb_status_id = 8,
							updated_at = clock_timestamp()
							WHERE id = v_record2.schema_id
							AND tb_databases_id = v_record.database_id
							AND tb_status_id IN (5,6,7);
							
							RETURN QUERY
								SELECT	DISTINCT
										'DROP SCHEMA'::VARCHAR AS object_type
										, CONCAT(v_record.database_name,'_',v_record2.schema_name)::VARCHAR AS object_name
										, v_status_from AS object_status_from
										, v_status_to AS object_status_to;
						END IF;
					END IF;
				END LOOP;
			END LOOP;
		END LOOP;

		-- ==================================================
		-- SCHEMAS
		-- 
		-- Este loop é responsável por DROPAR toda a cadeia de objetos que está por baixo da hierarquia de tb_schemas com tb_status_id = 7 (EM REMOÇÃO DATA PLATFORM)
		-- Para garantir segurança e respeitar principalmente tabelas de auditoria (localizadas em _hstlog), o DROP não poderá ser CASCADE.
		-- Teremos que efetuar o DROP subindo a escala de hierarquias (tb_tables + tb_columns -> tb_databases + tb_schemas)
		-- O novo status do objeto deverá ser 8 (REMOVIDO DATA PLATFORM)
		-- Esta ação excluirá todos os objetos dependentes na hirearquia.
		-- ==================================================

		FOR v_record IN
			SELECT	database_id
					, database_name
					, schema_id
					, schema_name
					, array_agg(table_id) AS tables_id
					, array_agg(table_name) AS tables_name
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (5,6,7)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id = 7
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id IN (5,6,7)
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id  IN (5,6,7)
			GROUP BY 1,2,3,4
		LOOP
			FOR v_record2 IN
				SELECT UNNEST(v_record.tables_name) AS table_name
			LOOP
				v_cmd := $cmd$
					DROP TABLE IF EXISTS "$cmd$ || CONCAT(v_record.database_name,'_',v_record.schema_name) || $cmd$"."$cmd$ || v_record2.table_name || $cmd$" CASCADE;
				$cmd$;
	
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
	
					UPDATE data_catalog.tb_tables SET
					tb_status_id = 8,
					updated_at = clock_timestamp()
					WHERE tb_databases_id = v_record.database_id
					AND tb_schemas_id = v_record.schema_id
					AND id = ANY(v_record.tables_id)
					AND tb_status_id IN (5,6,7);
					
					UPDATE data_catalog.tb_columns SET
					tb_status_id = 8,
					updated_at = clock_timestamp()
					WHERE tb_databases_id = v_record.database_id
					AND tb_schemas_id = v_record.schema_id
					AND tb_tables_id = ANY(v_record.tables_id)
					AND tb_status_id IN (5,6,7);
					
					RETURN QUERY
						SELECT	DISTINCT
								'DROP TABLE'::VARCHAR AS object_type
								, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record2.table_name)::VARCHAR AS object_name
								, v_status_from AS object_status_from
								, v_status_to AS object_status_to;
				END IF;
			END LOOP;

			IF CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record2.table_name) NOT IN ('public','information_schema','data_catalog') THEN
				v_cmd := $cmd$
					DROP SCHEMA "$cmd$ || CONCAT(v_record.database_name,'_',v_record.schema_name) || $cmd$";
				$cmd$;
	
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
	
					UPDATE data_catalog.tb_schemas SET
					tb_status_id = 8,
					updated_at = clock_timestamp()
					WHERE id = v_record.schema_id
					AND tb_databases_id = v_record.database_id
					AND tb_status_id = 7;
					
					RETURN QUERY
						SELECT	DISTINCT
								'DROP SCHEMA'::VARCHAR AS object_type
								, CONCAT(v_record.database_name,'_',v_record.schema_name)::VARCHAR AS object_name
								, v_status_from AS object_status_from
								, v_status_to AS object_status_to;
				END IF;
			END IF;
		END LOOP;

		-- ==================================================
		-- TABLES
		-- 
		-- Este loop é responsável por DROPAR toda a cadeia de objetos que está por baixo da hierarquia de tb_tables com tb_status_id = 7 (EM REMOÇÃO DATA PLATFORM)
		-- O novo status do objeto deverá ser 8 (REMOVIDO DATA PLATFORM)
		-- Esta ação excluirá todos os objetos dependentes na hirearquia.
		-- ==================================================

		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, schema_id
					, schema_name
					, table_id
					, table_name
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (5,6,7)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id IN (5,6,7)
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id = 7
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id  IN (5,6,7)
		LOOP
			v_cmd := $cmd$
				DROP TABLE IF EXISTS "$cmd$ || CONCAT(v_record.database_name,'_',v_record.schema_name) || $cmd$"."$cmd$ || v_record.table_name || $cmd$" CASCADE;
			$cmd$;

			IF v_cmd IS NOT NULL THEN
				EXECUTE v_cmd;

				UPDATE data_catalog.tb_tables SET
				tb_status_id = 8,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = v_record.database_id
				AND tb_schemas_id = v_record.schema_id
				AND id = v_record.table_id
				AND tb_status_id = 7;
				
				UPDATE data_catalog.tb_columns SET
				tb_status_id = 8,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = v_record.database_id
				AND tb_schemas_id = v_record.schema_id
				AND tb_tables_id = v_record.table_id
				AND tb_status_id IN (5,6,7);
				
				RETURN QUERY
					SELECT	DISTINCT
							'DROP TABLE'::VARCHAR AS object_type
							, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name)::VARCHAR AS object_name
							, v_status_from AS object_status_from
							, v_status_to AS object_status_to;
			END IF;
		END LOOP;

		-- ==================================================
		-- COLUMNS
		-- 
		-- Este loop é responsável por DROPAR colunas com tb_status_id = 7 (EM REMOÇÃO DATA PLATFORM)
		-- O novo status do objeto deverá ser 8 (REMOVIDO DATA PLATFORM)
		-- ==================================================

		FOR v_record IN
			SELECT	DISTINCT
					database_id
					, database_name
					, schema_id
					, schema_name
					, table_id
					, table_name
					, column_id
					, column_name
					, column_description
			FROM data_catalog.vw_catalog
			WHERE database_active IS TRUE
			AND database_description IS NOT NULL
			AND database_status_id IN (5,6,7)
			AND schema_active IS TRUE
			AND schema_description IS NOT NULL
			AND schema_status_id IN (5,6,7)
			AND table_active IS TRUE
			AND table_description IS NOT NULL
			AND table_payload_period_id IS NOT NULL
			AND table_status_id IN (5,6,7)
			AND column_active IS TRUE
			AND column_description IS NOT NULL
			AND column_data_type IS NOT NULL
			AND column_status_id = 7
		LOOP
			v_cmd := $cmd$
				ALTER TABLE IF EXISTS "$cmd$ || CONCAT(v_record.database_name,'_',v_record.schema_name) || $cmd$"."$cmd$ || v_record.table_name || $cmd$"
				DROP COLUMN IF EXISTS "$cmd$ || v_record.column_name || $cmd$";
			$cmd$;

			IF v_cmd IS NOT NULL THEN
				EXECUTE v_cmd;

				UPDATE data_catalog.tb_columns SET
				tb_status_id = 8,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = v_record.database_id
				AND tb_schemas_id = v_record.schema_id
				AND tb_tables_id = v_record.table_id
				AND id = v_record.column_id
				AND tb_status_id = 7;
				
				RETURN QUERY
					SELECT	DISTINCT
							'DROP COLUMN'::VARCHAR AS object_type
							, CONCAT(v_record.database_name,'_',v_record.schema_name,'.',v_record.table_name,'.',v_record.column_name)::VARCHAR AS object_name
							, v_status_from AS object_status_from
							, v_status_to AS object_status_to;
			END IF;
		END LOOP;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------
-- Determina o status de origem e destino com base no status atual do objeto. -- Cada bloco representa uma transição válida no fluxo de governança.
-- Bloco default
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
	ELSE
	-- ----------------------------------------------------------------
	-- Retorno default para caso nenhuma condição atenda
		RETURN QUERY
			SELECT	null AS object_type
					, null AS object_name
					, null AS object_status_from
					, null AS object_status_to;
	END IF;

	RETURN;
END; 
$BODY$;
