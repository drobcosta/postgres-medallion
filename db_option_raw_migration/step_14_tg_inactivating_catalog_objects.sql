CREATE OR REPLACE FUNCTION data_catalog.tg_inactivating_catalog_objects()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
DECLARE v_field_rules INTEGER;
BEGIN
	/*
	<< description >>
	*/
	IF TG_OP = 'UPDATE' THEN
		IF NEW.active IS FALSE THEN
			IF TG_ARGV[0] IS NULL THEN
				RETURN NULL;
			ELSIF TG_ARGV[0] = 'databases' THEN
				UPDATE data_catalog.tb_columns SET
				active = FALSE,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = NEW.id;

				UPDATE data_catalog.tb_tables SET
				active = FALSE,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = NEW.id;

				UPDATE data_catalog.tb_schemas SET
				active = FALSE,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = NEW.id;
				
				RETURN NEW;
			ELSIF TG_ARGV[0] = 'schemas' THEN
				UPDATE data_catalog.tb_columns SET
				active = FALSE,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = NEW.tb_databases_id
				AND tb_schemas_id = NEW.id;

				UPDATE data_catalog.tb_tables SET
				active = FALSE,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = NEW.tb_databases_id
				AND tb_schemas_id = NEW.id;

				RETURN NEW;
			ELSIF TG_ARGV[0] = 'tables' THEN
				UPDATE data_catalog.tb_columns SET
				active = FALSE,
				updated_at = clock_timestamp()
				WHERE tb_databases_id = NEW.tb_databases_id
				AND tb_schemas_id = NEW.id;

				RETURN NEW;
			ELSIF TG_ARGV[0] = 'columns' THEN
				RETURN NEW;
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

COMMENT ON FUNCTION data_catalog.tg_inactivating_catalog_objects()
    IS 'Função trigger responsável por tornar dinâmica a mudança da coluna active para todos os objetos de acordo com a hierarquia deles. Se um banco de dados é inativado, seus schemas, tabelas e colunas também são. Se um schema é inativado, suas tabelas e colunas também são. Se uma tabela é inativada, suas colunas também são.';