CREATE OR REPLACE FUNCTION data_catalog.tg_object_description_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER AS $BODY$
DECLARE v_record RECORD;
DECLARE v_cmd TEXT;
DECLARE v_object_description TEXT;
BEGIN
	IF ((NEW.description != OLD.description) AND (OLD.tb_status_id = NEW.tb_status_id) AND (NEW.tb_status_id = 5)) THEN
		IF TG_ARGV[0] = 'databases' THEN
			FOR v_record IN
				SELECT	DISTINCT
						database_id
						, database_name
						, database_description
						, schema_id
						, schema_name
						, schema_description
						, bronze_schema_name
				FROM data_catalog.vw_catalog
				WHERE database_id = NEW.id
			LOOP
				v_object_description := 'DATABASE = ' || NEW.name || ' (' || NEW.description || ') | SCHEMA = ' || v_record.schema_name || ' (' || v_record.schema_description || ')';
				v_cmd := $cmd$
					COMMENT ON SCHEMA $cmd$ || v_record.bronze_schema_name || $cmd$ IS '$cmd$ || v_object_description || $cmd$';
				$cmd$;
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
				END IF;
			END LOOP;
		ELSIF TG_ARGV[0] = 'schemas' THEN
			FOR v_record IN
				SELECT	DISTINCT
						database_id
						, database_name
						, database_description
						, schema_id
						, schema_name
						, schema_description
						, bronze_schema_name
				FROM data_catalog.vw_catalog
				WHERE database_id = NEW.tb_databases_id
				AND schema_id = NEW.id
			LOOP
				v_object_description := 'DATABASE = ' || v_record.database_name || ' (' || v_record.database_description || ') | SCHEMA = ' || NEW.name || ' (' || NEW.description || ')';
				v_cmd := $cmd$
					COMMENT ON SCHEMA $cmd$ || v_record.bronze_schema_name || $cmd$ IS '$cmd$ || v_object_description || $cmd$';
				$cmd$;
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
				END IF;
			END LOOP;
		ELSIF TG_ARGV[0] = 'tables' THEN
			FOR v_record IN
				SELECT	DISTINCT
						database_id
						, database_name
						, database_description
						, schema_id
						, schema_name
						, schema_description
						, table_id
						, table_name
						, table_description
						, bronze_path
				FROM data_catalog.vw_catalog
				WHERE database_id = NEW.tb_databases_id
				AND schema_id = NEW.tb_schemas_id
				AND table_id = NEW.id
			LOOP
				v_cmd := $cmd$
					COMMENT ON TABLE $cmd$ || v_record.bronze_path || $cmd$ IS '$cmd$ || NEW.description || $cmd$';
				$cmd$;
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
				END IF;
			END LOOP;
		ELSIF TG_ARGV[0] = 'columns' THEN
			FOR v_record IN
				SELECT	DISTINCT
						database_id
						, database_name
						, database_description
						, schema_id
						, schema_name
						, schema_description
						, table_id
						, table_name
						, table_description
						, column_id
						, column_name
						, column_description
						, bronze_path
				FROM data_catalog.vw_catalog
				WHERE database_id = NEW.tb_databases_id
				AND schema_id = NEW.tb_schemas_id
				AND table_id = NEW.tb_tables_id
				AND column_id = NEW.id
			LOOP
				v_cmd := $cmd$
					COMMENT ON COLUMN $cmd$ || v_record.bronze_path || $cmd$."$cmd$ || NEW.name || $cmd$" IS '$cmd$ || NEW.description || $cmd$';
				$cmd$;
				IF v_cmd IS NOT NULL THEN
					EXECUTE v_cmd;
				END IF;
			END LOOP;
		END IF;
	END IF;

	RETURN NEW;
END; $BODY$;
