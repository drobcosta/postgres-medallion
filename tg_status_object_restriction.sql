CREATE OR REPLACE FUNCTION data_catalog.tg_status_object_restriction()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
DECLARE v_field_rules INTEGER;
BEGIN
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

	IF OLD.tb_status_id = 1 AND NEW.tb_status_id IN (1,2,3) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "NOVO OBJETO (ANALISAR)"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> NOVO OBJETO (ANALISAR)
		---> APROVADO DATA PLATFORM
		---> REPROVADO DATA PLATFORM
		*/
		RETURN NEW;
	ELSIF OLD.tb_status_id = 2 AND NEW.tb_status_id IN (4) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "APROVADO DATA PLATFORM"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> EM CRIAÇÃO DATA PLATFORM
		*/
		RETURN NEW;
	ELSIF OLD.tb_status_id = 3 AND NEW.tb_status_id IN (1,2) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "REPROVADO DATA PLATFORM"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> NOVO OBJETO (ANALISAR)
		---> APROVADO DATA PLATFORM
		*/
		RETURN NEW;
	ELSIF OLD.tb_status_id = 4 AND NEW.tb_status_id IN (5) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "EM CRIAÇÃO DATA PLATFORM"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> DISPONÍVEL DATA PLATFORM
		*/
		RETURN NEW;
	ELSIF OLD.tb_status_id = 5 AND NEW.tb_status_id IN (6) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "DISPONÍVEL DATA PLATFORM"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> APROVADO PARA REMOÇÃO DATA PLATFORM
		*/
		RETURN NEW;
	ELSIF OLD.tb_status_id = 6 AND NEW.tb_status_id IN (7) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "APROVADO PARA REMOÇÃO DATA PLATFORM"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> EM REMOÇÃO DATA PLATFORM
		*/
		RETURN NEW;
	ELSIF OLD.tb_status_id = 7 AND NEW.tb_status_id IN (8) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "EM REMOÇÃO DATA PLATFORM"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> REMOVIDO DATA PLATFORM
		*/
		RETURN NEW;
	ELSIF OLD.tb_status_id = 8 AND NEW.tb_status_id IN (1,2) AND NEW.description IS NOT NULL THEN
		/*
		SE STATUS DO OBJETO FOR "REMOVIDO DATA PLATFORM"
		PERMITE UPDATE APENAS PARA OS STATUS:
		---> NOVO OBJETO (ANALISAR)
		---> APROVADO DATA PLATFORM
		*/
		RETURN NEW;
	ELSE
		RETURN NULL;
	END IF;
	RETURN NULL;
END; $BODY$;
COMMENT ON FUNCTION data_catalog.tg_status_object_restriction() IS 'Função responsável por validar regras para a mudança correta entre os status (tb_status_id) de cada tabela de objetos (tb_databases, tb_schemas, tb_tables e tb_columns). Também responsável por restringir a mudança incorreta entre os status (tb_status_id) de cada tabela de objetos (tb_databases, tb_schemas, tb_tables e tb_columns) obedecendo a ordem exata de mudança de status.';
