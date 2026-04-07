CREATE OR REPLACE FUNCTION data_catalog.tg_catalog_object_status_change_hierarchy()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
DECLARE v_field_rules INTEGER;
BEGIN
	/*
	Função responsável por tornar dinâmica a mudança de tb_status_id para todos os objetos de acordo com a hierarquia deles. 
	Só é válida para updates nos status 2, 3 e 6. 
	
	Mudanças em tb_databases para o status 3 ou 6 alterará todos os objetos que dependem de tb_databases. 
	
	Mudanças em tb_schemas para o status 2 também forçará a mudança para o status 2 para o tb_databases caso o tb_status_id de tb_databases seja 1. 
	Mudanças em tb_schemas para os status 3 ou 6 vai alterar os objetos dependentes a ele desde que estejam nos tb_status_id 1 (para mudanças para tb_status_id 3) e 5 (para mudanças para tb_status_id 6). 
	
	Tabelas e colunas seguem o mesmo padrão. 
	Para tb_status_id = 2, muda a hierarquia superior. 
	Para tb_status_id 3 ou 6, muda a hierarquia inferior.

	tb_columns só é alterado para tb_status_id = 2, pois não possui objetos que dependem dele.
	*/

	/*
	REGRAS DE VALIDAÇÃO DE CAMPOS:
	- tb_databases.description NÃO DEVE SER NULL
	- tb_schemas.description NÃO DEVE SER NULL
	- tb_tables.description NÃO DEVE SER NULL
	- tb_tables.tb_payload_period_id NÃO DEVE SER NULL
	- tb_columns.description NÃO DEVE SER NULL
	- tb_columns.data_type NÃO DEVE SER NULL
	*/
	SELECT COUNT(*)
	FROM (
		SELECT key, value 
		FROM jsonb_each_text(to_jsonb(NEW))
		WHERE key IN ('description', 'tb_payload_period_id', 'data_type')
	) rules
	WHERE value IS NULL
	INTO v_field_rules; 

	IF v_field_rules > 0 THEN
		RETURN NULL;
	END IF;

	IF TG_OP = 'UPDATE' THEN
		IF NEW.tb_status_id IN (2,3,6) THEN
			IF TG_ARGV[0] IS NULL THEN
				RETURN NULL;
			ELSIF TG_ARGV[0] = 'databases' THEN
				IF NEW.tb_status_id = 3 THEN
					UPDATE data_catalog.tb_schemas SET 
					tb_status_id = 3,
					description = COALESCE(description, CONCAT('[automatic description process] schema ', name, ' from database ', NEW.name)),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.id;
	
					UPDATE data_catalog.tb_tables SET 
					tb_status_id = 3,
					description = COALESCE(description, CONCAT('[automatic description process] table ', name, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					tb_payload_period_id = COALESCE(tb_payload_period_id, (SELECT max(id) FROM data_catalog.tb_payload_period WHERE "name" = 'Every day')),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.id;
	
					UPDATE data_catalog.tb_columns SET 
					tb_status_id = 3,
					description = COALESCE(description, CONCAT('[automatic description process] coluna ', name, ' from table ', tb_tables_id, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					data_type = COALESCE(data_type,'TEXT'),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.id;

					RETURN NEW;
				ELSIF NEW.tb_status_id = 6 THEN
					UPDATE data_catalog.tb_schemas SET 
					tb_status_id = 6,
					description = COALESCE(description, CONCAT('[automatic description process] schema ', name, ' from database ', NEW.name)),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.id
					AND tb_status_id = 5;
	
					UPDATE data_catalog.tb_tables SET 
					tb_status_id = 6,
					description = COALESCE(description, CONCAT('[automatic description process] table ', name, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					tb_payload_period_id = COALESCE(tb_payload_period_id, (SELECT max(id) FROM data_catalog.tb_payload_period WHERE "name" = 'Every day')),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.id
					AND tb_status_id = 5;
	
					UPDATE data_catalog.tb_columns SET 
					tb_status_id = 6,
					description = COALESCE(description, CONCAT('[automatic description process] coluna ', name, ' from table ', tb_tables_id, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					data_type = COALESCE(data_type,'TEXT'),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.id
					AND tb_status_id = 5;
	
					RETURN NEW;
				ELSE
					RETURN NEW;
				END IF;
			ELSIF TG_ARGV[0] = 'schemas' THEN
				IF NEW.tb_status_id = 2 THEN
					UPDATE data_catalog.tb_databases SET 
					tb_status_id = 2,
					description = COALESCE(description, CONCAT('[automatic description process] database ', name)),
					updated_at = clock_timestamp()
					WHERE id = NEW.tb_databases_id
					AND tb_status_id IN (1,3,8);
	
					RETURN NEW;
				ELSIF NEW.tb_status_id = 3 THEN
					UPDATE data_catalog.tb_tables SET 
					tb_status_id = 3,
					description = COALESCE(description, CONCAT('[automatic description process] table ', name, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					tb_payload_period_id = COALESCE(tb_payload_period_id, (SELECT max(id) FROM data_catalog.tb_payload_period WHERE "name" = 'Every day')),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND tb_schemas_id = NEW.id;
	
					UPDATE data_catalog.tb_columns SET 
					tb_status_id = 3,
					description = COALESCE(description, CONCAT('[automatic description process] coluna ', name, ' from table ', tb_tables_id, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					data_type = COALESCE(data_type,'TEXT'),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND tb_schemas_id = NEW.id;
	
					RETURN NEW;
				ELSIF NEW.tb_status_id = 6 THEN
					UPDATE data_catalog.tb_tables SET 
					tb_status_id = 6,
					description = COALESCE(description, CONCAT('[automatic description process] table ', name, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					tb_payload_period_id = COALESCE(tb_payload_period_id, (SELECT max(id) FROM data_catalog.tb_payload_period WHERE "name" = 'Every day')),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND tb_schemas_id = NEW.id
					AND tb_status_id = 5;
	
					UPDATE data_catalog.tb_columns SET 
					tb_status_id = 6,
					description = COALESCE(description, CONCAT('[automatic description process] coluna ', name, ' from table ', tb_tables_id, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					data_type = COALESCE(data_type,'TEXT'),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND tb_schemas_id = NEW.id
					AND tb_status_id = 5;
	
					RETURN NEW;
				ELSE
					RETURN NEW;
				END IF;
			ELSIF TG_ARGV[0] = 'tables' THEN
				IF NEW.tb_status_id = 2 THEN
					UPDATE data_catalog.tb_databases SET 
					tb_status_id = 2,
					description = COALESCE(description, CONCAT('[automatic description process] database ', name)),
					updated_at = clock_timestamp()
					WHERE id = NEW.tb_databases_id
					AND tb_status_id IN (1,3,8);
	
					UPDATE data_catalog.tb_schemas SET 
					tb_status_id = 2,
					description = COALESCE(description, CONCAT('[automatic description process] schema ', name, ' from database ', tb_databases_id)),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND id = NEW.tb_schemas_id
					AND tb_status_id IN (1,3,8);
	
					RETURN NEW;
				ELSIF NEW.tb_status_id = 3 THEN
					UPDATE data_catalog.tb_columns SET 
					tb_status_id = 3,
					description = COALESCE(description, CONCAT('[automatic description process] coluna ', name, ' from table ', tb_tables_id, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					data_type = COALESCE(data_type,'TEXT'),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND tb_schemas_id = NEW.tb_schemas_id
					AND tb_tables_id = NEW.id;
	
					RETURN NEW;
				ELSIF NEW.tb_status_id = 6 THEN
					UPDATE data_catalog.tb_columns SET 
					tb_status_id = 6,
					description = COALESCE(description, CONCAT('[automatic description process] coluna ', name, ' from table ', tb_tables_id, ' from schema ', tb_schemas_id, ' from database ', NEW.name)),
					data_type = COALESCE(data_type,'TEXT'),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND tb_schemas_id = NEW.tb_schemas_id
					AND tb_tables_id = NEW.id
					AND tb_status_id = 5;
	
					RETURN NEW;
				ELSE
					RETURN NEW;
				END IF;
			ELSIF TG_ARGV[0] = 'columns' THEN
				IF NEW.tb_status_id = 2 THEN
					UPDATE data_catalog.tb_databases SET 
					tb_status_id = 2,
					description = COALESCE(description, CONCAT('[automatic description process] database ', name)),
					updated_at = clock_timestamp()
					WHERE id = NEW.tb_databases_id
					AND tb_status_id IN (1,3,8);
	
					UPDATE data_catalog.tb_schemas SET 
					tb_status_id = 2,
					description = COALESCE(description, CONCAT('[automatic description process] schema ', name, ' from database ', tb_databases_id)),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND id = NEW.tb_schemas_id
					AND tb_status_id IN (1,3,8);
	
					UPDATE data_catalog.tb_tables SET 
					tb_status_id = 2,
					description = COALESCE(description, CONCAT('[automatic description process] table ', name, ' from schema ', tb_schemas_id, ' from database ', tb_databases_id)),
					tb_payload_period_id = COALESCE(tb_payload_period_id, (SELECT max(id) FROM data_catalog.tb_payload_period WHERE "name" = 'Every day')),
					updated_at = clock_timestamp()
					WHERE tb_databases_id = NEW.tb_databases_id
					AND tb_schemas_id = NEW.tb_schemas_id
					AND id = NEW.tb_tables_id
					AND tb_status_id IN (1,3);
	
					RETURN NEW;
				ELSE
					RETURN NEW;
				END IF;
			ELSE
				RETURN NULL;
			END IF;
		ELSE
			RETURN NEW;
		END IF;
	END IF;

	RETURN NULL;
END; 
$BODY$;

COMMENT ON FUNCTION data_catalog.tg_catalog_object_status_change_hierarchy()
    IS 'Função responsável por tornar dinâmica a mudança de tb_status_id para todos os objetos de acordo com a hierarquia deles. Só é válida para updates nos status 2, 3 e 6. Mudanças em tb_databases para o status 3 ou 6 alterará todos os objetos que dependem de tb_databases. Mudanças em tb_schemas para o status 2 também forçará a mudança para o status 2 para o tb_databases caso o tb_status_id de tb_databases seja 1. Mudanças em tb_schemas para os status 3 ou 6 vai alterar os objetos dependentes a ele desde que estejam nos tb_status_id 1 (para mudanças para tb_status_id 3) e 5 (para mudanças para tb_status_id 6). Tabelas e colunas seguem o mesmo padrão. Para tb_status_id = 2, muda a hierarquia superior. Para tb_status_id 3 ou 6, muda a hierarquia inferior.';
