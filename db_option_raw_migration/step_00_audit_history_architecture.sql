-- BLOCO QUE VAI REMOVER AS EVENT-TRIGGER FUNCTIONS E AS EVENT TRIGGERS PARA RECRIAR DO ZERO A ARQUITETURA DE AUDITORIA/HISTÓRICO DE TABELAS
DO $$
BEGIN
    IF EXISTS (SELECT evtname FROM pg_event_trigger WHERE evtname = 'tg_audit_tables_add') THEN
        DROP EVENT TRIGGER tg_audit_tables_add;
    END IF;

    IF EXISTS (SELECT evtname FROM pg_event_trigger WHERE evtname = 'tg_audit_tables_drop') THEN
        DROP EVENT TRIGGER tg_audit_tables_drop;
    END IF;

    IF EXISTS (SELECT evtname FROM pg_event_trigger WHERE evtname = 'tg_audit_schemas_drop') THEN
        DROP EVENT TRIGGER tg_audit_schemas_drop;
    END IF;
END $$;
DROP FUNCTION IF EXISTS tg_audit_tables_drop();
DROP FUNCTION IF EXISTS tg_audit_tables_add();

-- FUNÇÃO RESPONSÁVEL PELA CAPTAÇÃO DO EVENTO DE "CREATE TABLE", DESDE QUE NÃO SEJA UMA TABELA DE AUDITORIA/HISTÓRICO, PARA CRIAR SUA ESTRUTURA DE AUDITORIA/HISTÓRICO
-- A ESTRUTURA DE AUDITORIA/HISTÓRICO SERÁ AUTOMATICAMENTE CRIADA E GERENCIADA EM UM SCHEMA NOVO, COM O MESMO NOME DO SCHEMA DA TABELA QUE ESTÁ SENDO CRIADA PORÉM COM O SUFIXO DE _HSTLOG (EX.: PUBLIC -> PUBLIC_HSTLOG)
-- A TABELA RESPONSÁVEL PELA AUDITORIA/HISTÓRICO DA NOVA TABELA QUE ESTÁ SENDO CRIADA SERÁ PARTICIONADA EM 5 (SUFIXOS: _INSERT, _UPDATE, _DELETE, _TRUNCATE, _DEFAULT)
CREATE OR REPLACE FUNCTION public.tg_audit_tables_add()
    RETURNS event_trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
DECLARE sch_hstlog VARCHAR;
DECLARE sch VARCHAR;
DECLARE tbl VARCHAR;
DECLARE tblsuffix VARCHAR DEFAULT '_auditlog';
DECLARE parent VARCHAR;
DECLARE cmd VARCHAR;
DECLARE obj RECORD;
BEGIN
    SET client_min_messages = 'error';
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj.object_type = 'table' THEN
            sch := REPLACE(obj.schema_name,'"','');
            sch_hstlog := CONCAT(sch,tblsuffix);
            tbl := REPLACE(split_part(obj.object_identity,'.',2),'"','');
            SELECT inhparent::VARCHAR FROM pg_inherits WHERE inhrelid = obj.objid INTO parent;
            
            IF tbl NOT LIKE '%_' || tblsuffix AND parent IS NULL THEN
-- =============================================================
-- SCHEMA
-- =============================================================
                cmd := $cmd$
                    CREATE SCHEMA IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$";
                $cmd$;

                EXECUTE cmd;
-- =============================================================
-- TABLE
-- =============================================================
                cmd := $cmd$
                    CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" (
                        id bigserial,
                        "$cmd$ || tbl || $cmd$_old" jsonb,
                        "$cmd$ || tbl || $cmd$_new" jsonb,
                        tg_usr varchar,
                        tg_op varchar,
                        executed_at timestamp without time zone not null default current_timestamp
                    ) PARTITION BY LIST(tg_op);
                    CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_insert"
                        PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"
                        FOR VALUES IN ('INSERT');
                    CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_update"
                        PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"
                        FOR VALUES IN ('UPDATE');
                    CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_delete"
                        PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"
                        FOR VALUES IN ('DELETE');
                    CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_truncate"
                        PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"
                        FOR VALUES IN ('TRUNCATE');
                    CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_default"
                        PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"
                        DEFAULT;
                    CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_tg_op"
                        ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" (tg_op, executed_at);
                    CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_insert_brin"
                        ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_insert" USING BRIN (executed_at)
                        WITH (pages_per_range = 128);
                    CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_update_brin"
                        ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_update" USING BRIN (executed_at)
                        WITH (pages_per_range = 128);
                    CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_delete_brin"
                        ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_delete" USING BRIN (executed_at)
                        WITH (pages_per_range = 128);
                    CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_truncate_brin"
                        ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_truncate" USING BRIN (executed_at)
                        WITH (pages_per_range = 128);
                    CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_default_brin"
                        ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_default" USING BRIN (executed_at)
                        WITH (pages_per_range = 128);
                $cmd$;
                EXECUTE cmd;
-- =============================================================
-- FUNCTION TRIGGER
-- =============================================================
                cmd := $cmd$
                    CREATE OR REPLACE FUNCTION "$cmd$ || sch_hstlog || $cmd$"."tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"()
                    RETURNS TRIGGER
                    SECURITY INVOKER
                    LANGUAGE PLPGSQL
                    AS $tg$
                    BEGIN
                        IF TG_OP = 'INSERT' THEN
                            INSERT INTO "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" ("$cmd$ || tbl || $cmd$_old", "$cmd$ || tbl || $cmd$_new", tg_usr, tg_op, executed_at)
                            VALUES (NULL, row_to_json(NEW.*), current_user, TG_OP, clock_timestamp());
                        ELSE
                            INSERT INTO "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" ("$cmd$ || tbl || $cmd$_old", "$cmd$ || tbl || $cmd$_new", tg_usr, tg_op, executed_at)
                            VALUES (row_to_json(OLD.*), row_to_json(NEW.*), current_user, TG_OP, clock_timestamp());
                        END IF;
                       
                        RETURN NEW;
                    END; $tg$;
                $cmd$;
               EXECUTE cmd;
-- =============================================================
-- TRIGGER
-- =============================================================
                IF NOT EXISTS (
                    SELECT tg.tgname, sc.nspname, tb.relname 
                    FROM pg_trigger tg 
                    JOIN pg_class tb ON tb.oid = tg.tgrelid 
                    JOIN pg_namespace sc ON sc.oid = tb.relnamespace
                    WHERE sc.nspname = sch
                    AND tb.relname = tbl
                    AND tg.tgname = CONCAT('tg_',tbl,tblsuffix)
                ) THEN
                    cmd := $cmd$
                        CREATE TRIGGER "tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" AFTER INSERT OR UPDATE OR DELETE ON "$cmd$ || sch || $cmd$"."$cmd$ || tbl || $cmd$"
                        FOR EACH ROW EXECUTE PROCEDURE "$cmd$ || sch_hstlog || $cmd$"."tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"();
                    $cmd$;
                    EXECUTE cmd;
                END IF;
-- =============================================================
-- VIEW
-- =============================================================
                cmd := $cmd$
                    CREATE OR REPLACE VIEW "$cmd$ || sch_hstlog || $cmd$"."vw_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" AS (
                        WITH changes AS (
                            SELECT  id,
                                    "$cmd$ || tbl || $cmd$_old",
                                    "$cmd$ || tbl || $cmd$_new",
                                    (
                                        SELECT json_object_agg(COALESCE(old.key, new.key), old.value)
                                        FROM json_each_text("$cmd$ || tbl || $cmd$_old"::json) old
                                        FULL OUTER JOIN json_each_text("$cmd$ || tbl || $cmd$_new"::json) new ON new.key = old.key
                                        WHERE new.value IS DISTINCT FROM old.value
                                    ) AS diff,
                                    tg_usr,
                                    tg_op,
                                    executed_at
                            FROM "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"
                        ), changes_and_fields AS (
                            SELECT  id,
                                    "$cmd$ || tbl || $cmd$_old",
                                    "$cmd$ || tbl || $cmd$_new",
                                    (SELECT array_agg(fields) FROM json_object_keys(diff) fields) AS mod_fields,
                                    tg_usr,
                                    tg_op,
                                    executed_at
                            FROM changes
                        )
                        SELECT  id,
                                "$cmd$ || tbl || $cmd$_old",
                                "$cmd$ || tbl || $cmd$_new",
                                unnest(mod_fields) AS mod_field,
                                tg_usr,
                                tg_op,
                                executed_at
                        FROM changes_and_fields
                    );
                $cmd$;
                EXECUTE cmd;
            END IF;
        END IF;
    END LOOP;

END; 
$BODY$;

-- FUNÇÃO RESPONSÁVEL POR REMOVER A ESTRUTURA DE AUDITORIA/HISTÓRICO DE TABELAS QUE ESTÃO SENDO REMOVIDAS DO AMBIENTE
CREATE OR REPLACE FUNCTION public.tg_audit_tables_drop()
    RETURNS event_trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
DECLARE sch_hstlog VARCHAR;
DECLARE sch VARCHAR;
DECLARE tbl VARCHAR;
DECLARE tblsuffix VARCHAR DEFAULT '_auditlog';
DECLARE cmd VARCHAR;
DECLARE obj RECORD;
BEGIN
    SET client_min_messages = 'error';
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'table' THEN
            sch := REPLACE(obj.schema_name,'"','');
            sch_hstlog := CONCAT(sch,tblsuffix); -- NOVIDADE
            tbl := REPLACE(split_part(obj.object_identity,'.',2),'"','');

            IF tbl NOT LIKE '%_' || tblsuffix THEN
-- =============================================================
-- VIEW
-- =============================================================
                cmd := $cmd$
                DROP VIEW IF EXISTS "$cmd$ || sch_hstlog || $cmd$"."vw_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$";
                $cmd$;
                EXECUTE cmd;
-- =============================================================
-- TRIGGER
-- =============================================================
                cmd := $cmd$
                DROP TRIGGER IF EXISTS "tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || tbl || $cmd$";
                $cmd$;
                EXECUTE cmd;
-- =============================================================
-- FUNCTION TRIGGER
-- =============================================================
                cmd := $cmd$
                DROP FUNCTION IF EXISTS "$cmd$ || sch_hstlog || $cmd$"."tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"();
                $cmd$;
                EXECUTE cmd;
-- =============================================================
-- TABLE
-- =============================================================
                cmd := $cmd$
                DROP TABLE IF EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$";
                $cmd$;
                EXECUTE cmd;
            END IF;
        ELSIF obj.object_type = 'schema' THEN
            IF obj.object_name NOT ILIKE '%_' || tblsuffix THEN
                cmd := $cmd$
                    DROP SCHEMA IF EXISTS "$cmd$ || CONCAT(obj.object_name,tblsuffix) || $cmd$";
                $cmd$;
                EXECUTE cmd;
            END IF;
        END IF;
    END LOOP;
END; 
$BODY$;

-- EVENT TRIGGER PARA O EVENTO DE CREATE TABLE CHAMANDO A EVENT-TRIGGER FUNCTION tg_audit_tables_add()
CREATE EVENT TRIGGER tg_audit_tables_add
ON ddl_command_end
WHEN tag IN ('CREATE TABLE')
EXECUTE PROCEDURE tg_audit_tables_add();

-- EVENT TRIGGER PARA O EVENTO DE DROP TABLE CHAMANDO A EVENT-TRIGGER FUNCTION tg_audit_tables_drop()
CREATE EVENT TRIGGER tg_audit_tables_drop
ON sql_drop
WHEN tag IN ('DROP TABLE')
EXECUTE PROCEDURE tg_audit_tables_drop();

-- EVENT TRIGGER PARA O EVENTO DE DROP SCHEMA CHAMANDO A EVENT-TRIGGER FUNCTION tg_audit_tables_drop()
CREATE EVENT TRIGGER tg_audit_schemas_drop
ON sql_drop
WHEN tag IN ('DROP SCHEMA')
EXECUTE PROCEDURE tg_audit_tables_drop();

-- FUNÇÃO OPCIONAL PARA CRIAR A ESTRUTURA AUDITLOG MANUAL CASA NÃO DESEJE UTILIZAR EVENT TRIGGER
CREATE OR REPLACE FUNCTION public.fn_audit_tables_add(
	p_table character varying)
    RETURNS TABLE(object_name character varying, object_type character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE sch_hstlog VARCHAR;
DECLARE sch VARCHAR;
DECLARE tbl VARCHAR;
DECLARE tblsuffix VARCHAR DEFAULT '_auditlog';
DECLARE parent VARCHAR;
DECLARE cmd VARCHAR;
DECLARE obj RECORD;
BEGIN
    SET client_min_messages = 'error';
-- ===================================================================================
	IF EXISTS(SELECT relname FROM pg_class WHERE relname = p_table AND relkind = 'r') THEN
		SELECT s.nspname FROM pg_namespace s JOIN pg_class t ON t.relnamespace = s.oid AND t.relname = p_table AND t.relkind = 'r' INTO sch;
		sch_hstlog := CONCAT(sch,tblsuffix);
		tbl := p_table;
		SELECT tp.inhparent::VARCHAR FROM pg_inherits tp JOIN pg_class t ON t.oid = tp.inhrelid WHERE t.relname = p_table INTO parent;
		
		IF tbl NOT LIKE '%_' || tblsuffix AND parent IS NULL THEN
		-- =============================================================
		-- SCHEMA
		-- =============================================================
			cmd := $cmd$
			CREATE SCHEMA IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$";
			$cmd$;
			
			EXECUTE cmd;
			RETURN QUERY
			SELECT sch_hstlog::VARCHAR AS object_name, 'SCHEMA'::VARCHAR AS object_type;
		-- =============================================================
		-- TABLE
		-- =============================================================
		cmd := $cmd$
			CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" (
				id bigserial,
				"$cmd$ || tbl || $cmd$_old" jsonb,
				"$cmd$ || tbl || $cmd$_new" jsonb,
				tg_usr varchar,
				tg_op varchar,
				executed_at timestamp without time zone not null default current_timestamp
			) PARTITION BY LIST(tg_op);
			CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_insert"
				PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" FOR VALUES IN ('INSERT');
			CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_update" 
				PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" FOR VALUES IN ('UPDATE');
			CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_delete"
				PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" FOR VALUES IN ('DELETE');
			CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_truncate"
				PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" FOR VALUES IN ('TRUNCATE');
			CREATE TABLE IF NOT EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_default"
				PARTITION OF "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" DEFAULT;
			CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_tg_op"
				ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" (tg_op, executed_at);
			CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_insert_brin"
				ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_insert" USING BRIN (executed_at) WITH (pages_per_range = 128);
			CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_update_brin"
				ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_update" USING BRIN (executed_at) WITH (pages_per_range = 128);
			CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_delete_brin"
				ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_delete" USING BRIN (executed_at) WITH (pages_per_range = 128);
			CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_truncate_brin"
				ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_truncate" USING BRIN (executed_at) WITH (pages_per_range = 128);
			CREATE INDEX IF NOT EXISTS "idx_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_default_brin"
				ON "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$_default" USING BRIN (executed_at) WITH (pages_per_range = 128);
		$cmd$;
		EXECUTE cmd;
		RETURN QUERY
			SELECT CONCAT(tbl,tblsuffix)::VARCHAR AS object_name, 'TABLE'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_insert')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_update')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_delete')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_truncate')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_default')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_tg_op')::VARCHAR AS object_name, 'TRIGGER'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_insert_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_update_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_delete_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_truncate_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
			SELECT CONCAT(tbl,tblsuffix,'_default_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type;
		-- =============================================================
		-- FUNCTION TRIGGER
		-- =============================================================
		cmd := $cmd$
			CREATE OR REPLACE FUNCTION "$cmd$ || sch_hstlog || $cmd$"."tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"()
			RETURNS TRIGGER
			SECURITY INVOKER
			LANGUAGE PLPGSQL
			AS $tg$
			BEGIN
				IF TG_OP = 'INSERT' THEN
					INSERT INTO "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" ("$cmd$ || tbl || $cmd$_old", "$cmd$ || tbl || $cmd$_new", tg_usr, tg_op, executed_at)
					VALUES (NULL, row_to_json(NEW.*), current_user, TG_OP, clock_timestamp());
				ELSE
					INSERT INTO "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" ("$cmd$ || tbl || $cmd$_old", "$cmd$ || tbl || $cmd$_new", tg_usr, tg_op, executed_at)
					VALUES (row_to_json(OLD.*), row_to_json(NEW.*), current_user, TG_OP, clock_timestamp());
				END IF;
				
				RETURN NEW;
			END; $tg$;
		$cmd$;
		EXECUTE cmd;
		RETURN QUERY
			SELECT CONCAT('tg_',tbl,tblsuffix)::VARCHAR AS object_name, 'FUNCTION'::VARCHAR AS object_type;
		-- =============================================================
		-- TRIGGER
		-- =============================================================
		IF NOT EXISTS (
			SELECT tg.tgname, sc.nspname, tb.relname 
			FROM pg_trigger tg 
			JOIN pg_class tb ON tb.oid = tg.tgrelid 
			JOIN pg_namespace sc ON sc.oid = tb.relnamespace
			WHERE sc.nspname = sch
			AND tb.relname = tbl
			AND tg.tgname = CONCAT('tg_',tbl,tblsuffix)
		) THEN
			cmd := $cmd$
				CREATE TRIGGER "tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" AFTER INSERT OR UPDATE OR DELETE ON "$cmd$ || sch || $cmd$"."$cmd$ || tbl || $cmd$"
				FOR EACH ROW EXECUTE PROCEDURE "$cmd$ || sch_hstlog || $cmd$"."tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"();
			$cmd$;
			EXECUTE cmd;
			RETURN QUERY
				SELECT CONCAT('tg_',tbl,tblsuffix)::VARCHAR AS object_name, 'TRIGGER'::VARCHAR AS object_type;
		END IF;
		-- =============================================================
		-- VIEW
		-- =============================================================
		cmd := $cmd$
			CREATE OR REPLACE VIEW "$cmd$ || sch_hstlog || $cmd$"."vw_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" AS (
			WITH changes AS (
				SELECT  id,
						"$cmd$ || tbl || $cmd$_old",
						"$cmd$ || tbl || $cmd$_new",
						(
							SELECT json_object_agg(COALESCE(old.key, new.key), old.value)
							FROM json_each_text("$cmd$ || tbl || $cmd$_old"::json) old
							FULL OUTER JOIN json_each_text("$cmd$ || tbl || $cmd$_new"::json) new ON new.key = old.key
							WHERE new.value IS DISTINCT FROM old.value
						) AS diff,
						tg_usr,
						tg_op,
						executed_at
				FROM "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"
			), changes_and_fields AS (
				SELECT  id,
						"$cmd$ || tbl || $cmd$_old",
						"$cmd$ || tbl || $cmd$_new",
						(SELECT array_agg(fields) FROM json_object_keys(diff) fields) AS mod_fields,
						tg_usr,
						tg_op,
					executed_at
				FROM changes
			)
			SELECT  id,
					"$cmd$ || tbl || $cmd$_old",
					"$cmd$ || tbl || $cmd$_new",
					unnest(mod_fields) AS mod_field,
					tg_usr,
					tg_op,
					executed_at
			FROM changes_and_fields
			);
		$cmd$;
		EXECUTE cmd;
		RETURN QUERY
			SELECT CONCAT('vw_',tbl,tblsuffix)::VARCHAR AS object_name, 'VIEW'::VARCHAR AS object_type;
		END IF;
	END IF;

	RETURN;

END; 
$BODY$;

-- FUNÇÃO MANUAL PARA REMOVER A ESTRUTURA DE AUDITLOG SEM REMOVER A TABELA PRINCIPAL
CREATE OR REPLACE FUNCTION public.fn_audit_tables_drop(
	p_table character varying)
    RETURNS TABLE(object_name character varying, object_type character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE sch_hstlog VARCHAR;
DECLARE sch VARCHAR;
DECLARE tbl VARCHAR;
DECLARE tblsuffix VARCHAR DEFAULT '_auditlog';
DECLARE cmd VARCHAR;
DECLARE obj RECORD;
DECLARE parent VARCHAR;
BEGIN
    SET client_min_messages = 'error';

	SELECT s.nspname FROM pg_namespace s JOIN pg_class t ON t.relnamespace = s.oid AND t.relname = p_table AND t.relkind = 'r' INTO sch;
	sch_hstlog := CONCAT(sch,tblsuffix);
	tbl := p_table;
	SELECT tp.inhparent::VARCHAR FROM pg_inherits tp JOIN pg_class t ON t.oid = tp.inhrelid WHERE t.relname = p_table INTO parent;

	IF tbl NOT LIKE '%_' || tblsuffix AND parent IS NULL THEN
		IF EXISTS (
			SELECT t.relname
			FROM pg_class t
			JOIN pg_namespace s ON s.oid = t.relnamespace
			WHERE s.nspname = sch_hstlog
			AND t.relname = CONCAT(tbl,tblsuffix)
		) THEN
			-- DROP VIEW
			cmd := $cmd$
				DROP VIEW IF EXISTS "$cmd$ || sch_hstlog || $cmd$"."vw_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$";
			$cmd$;
			EXECUTE cmd;
			RETURN QUERY
				SELECT CONCAT('vw_',tbl,tblsuffix)::VARCHAR AS object_name, 'VIEW'::VARCHAR AS object_type;
			-- DROP TRIGGER
			cmd := $cmd$
				DROP TRIGGER IF EXISTS "tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" ON "$cmd$ || sch || $cmd$"."$cmd$ || tbl || $cmd$";
			$cmd$;
			EXECUTE cmd;
			RETURN QUERY
				SELECT CONCAT('tg_',tbl,tblsuffix)::VARCHAR AS object_name, 'TRIGGER'::VARCHAR AS object_type;
			-- DROP TRIGGER FUNCTION
			cmd := $cmd$
				DROP FUNCTION IF EXISTS "$cmd$ || sch || $cmd$"."tg_$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$"();
			$cmd$;
			EXECUTE cmd;
			RETURN QUERY
				SELECT CONCAT('tg_',tbl,tblsuffix)::VARCHAR AS object_name, 'FUNCTION'::VARCHAR AS object_type;
			-- DROP PARTITION TABLE
			cmd := $cmd$
				DROP TABLE IF EXISTS "$cmd$ || sch_hstlog || $cmd$"."$cmd$ || CONCAT(tbl,tblsuffix) || $cmd$" CASCADE;
			$cmd$;
			EXECUTE cmd;
			RETURN QUERY
				SELECT CONCAT(tbl,tblsuffix)::VARCHAR AS object_name, 'TABLE'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_insert')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_update')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_delete')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_truncate')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_default')::VARCHAR AS object_name, 'TABLE PARTITION'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_tg_op')::VARCHAR AS object_name, 'TRIGGER'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_insert_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_update_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_delete_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_truncate_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type UNION ALL
				SELECT CONCAT(tbl,tblsuffix,'_default_brin')::VARCHAR AS object_name, 'INDEX TYPE BRIN'::VARCHAR AS object_type;
		END IF;
	END IF;

	RETURN;

END; 
$BODY$;
