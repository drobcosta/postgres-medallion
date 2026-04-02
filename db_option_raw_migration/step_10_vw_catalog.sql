CREATE OR REPLACE VIEW data_catalog.vw_catalog
 AS
 SELECT database_id,
    database_name,
    database_description,
    database_status_id,
    database_status_name,
    database_active,
    schema_id,
    schema_name,
    schema_description,
    schema_status_id,
    schema_status_name,
    schema_active,
    table_id,
    table_name,
    table_description,
    table_status_id,
    table_status_name,
    table_payload_period_id,
    table_payload_period_name,
    table_payload_period_minutes,
    table_active,
    column_id,
    column_name,
    column_data_type,
    column_pk,
    column_description,
    column_status_id,
    column_status_name,
    column_active,
    bronze_schema_name,
    raw_schema_name,
    hstlog_schema_name,
    bronze_path,
    raw_path,
    hstlog_path,
    hstlog_insert_path,
    hstlog_update_path,
    hstlog_delete_path
   FROM ( SELECT d.id AS database_id,
            d.name AS database_name,
            d.description AS database_description,
            d.tb_status_id AS database_status_id,
            d_st.name AS database_status_name,
            d.active AS database_active,
            s.id AS schema_id,
            s.name AS schema_name,
            s.description AS schema_description,
            s.tb_status_id AS schema_status_id,
            s_st.name AS schema_status_name,
            s.active AS schema_active,
            t.id AS table_id,
            t.name AS table_name,
            t.description AS table_description,
            t.tb_status_id AS table_status_id,
            t_st.name AS table_status_name,
            t.tb_payload_period_id AS table_payload_period_id,
            t_p.name AS table_payload_period_name,
            t_p.minutes AS table_payload_period_minutes,
            t.active AS table_active,
            c.id AS column_id,
            c.name AS column_name,
            c.data_type AS column_data_type,
            c.is_pk AS column_pk,
            c.description AS column_description,
            c.tb_status_id AS column_status_id,
            c_st.name AS column_status_name,
            c.active AS column_active,
            concat('"', d.name, '_', s.name, '"') AS bronze_schema_name,
            concat('"', raw_sch.nspname, '"') AS raw_schema_name,
            concat('"', hstlog_sch.nspname, '"') AS hstlog_schema_name,
            concat('"', d.name, '_', s.name, '"."', t.name, '"') AS bronze_path,
            concat('"', raw_sch.nspname, '"."', t.name, '"') AS raw_path,
            concat('"', hstlog_sch.nspname, '"."', hstlog_tbl.relname, '"') AS hstlog_path,
            concat('"', hstlog_sch.nspname, '"."', hstlog_tbl_insert.relname, '"') AS hstlog_insert_path,
            concat('"', hstlog_sch.nspname, '"."', hstlog_tbl_update.relname, '"') AS hstlog_update_path,
            concat('"', hstlog_sch.nspname, '"."', hstlog_tbl_delete.relname, '"') AS hstlog_delete_path
           FROM data_catalog.tb_databases d
             LEFT JOIN data_catalog.tb_status d_st ON d_st.id = d.tb_status_id
             LEFT JOIN data_catalog.tb_schemas s ON s.tb_databases_id::text = d.id::text
             LEFT JOIN data_catalog.tb_status s_st ON s_st.id = s.tb_status_id
             LEFT JOIN data_catalog.tb_tables t ON t.tb_databases_id::text = d.id::text AND t.tb_schemas_id::text = s.id::text
             LEFT JOIN data_catalog.tb_status t_st ON t_st.id = t.tb_status_id
             LEFT JOIN data_catalog.tb_payload_period t_p ON t_p.id = t.tb_payload_period_id
             LEFT JOIN data_catalog.tb_columns c ON c.tb_databases_id::text = d.id::text AND c.tb_schemas_id::text = s.id::text AND c.tb_tables_id::text = t.id::text
             LEFT JOIN data_catalog.tb_status c_st ON c_st.id = c.tb_status_id
             LEFT JOIN pg_namespace raw_sch ON raw_sch.nspname <> concat(d.name, '_', s.name) AND raw_sch.nspname ~~ concat(d.name, '_', s.name, '%') AND (raw_sch.nspname ~~ ANY ( SELECT concat('%', trp.pattern) AS concat
                   FROM data_catalog.tb_raw_patterns trp))
             LEFT JOIN pg_namespace hstlog_sch ON hstlog_sch.nspname <> concat(d.name, '_', s.name) AND hstlog_sch.nspname <> raw_sch.nspname AND hstlog_sch.nspname ~~ concat(raw_sch.nspname, '%') AND (hstlog_sch.nspname ~~ ANY ( SELECT concat('%', trp.pattern, '_auditlog') AS concat
                   FROM data_catalog.tb_raw_patterns trp))
             LEFT JOIN pg_class hstlog_tbl ON hstlog_tbl.relkind = 'r'::"char" AND hstlog_tbl.relnamespace = hstlog_sch.oid AND hstlog_tbl.relname ~~ concat(t.name, '%') AND hstlog_tbl.relname ~~ '%_auditlog'::text
             LEFT JOIN pg_class hstlog_tbl_insert ON hstlog_tbl_insert.relkind = 'r'::"char" AND hstlog_tbl_insert.relnamespace = hstlog_sch.oid AND hstlog_tbl_insert.relname ~~ concat(t.name, '%') AND hstlog_tbl_insert.relname ~~ '%_insert'::text
             LEFT JOIN pg_class hstlog_tbl_update ON hstlog_tbl_update.relkind = 'r'::"char" AND hstlog_tbl_update.relnamespace = hstlog_sch.oid AND hstlog_tbl_update.relname ~~ concat(t.name, '%') AND hstlog_tbl_update.relname ~~ '%_update'::text
             LEFT JOIN pg_class hstlog_tbl_delete ON hstlog_tbl_delete.relkind = 'r'::"char" AND hstlog_tbl_delete.relnamespace = hstlog_sch.oid AND hstlog_tbl_delete.relname ~~ concat(t.name, '%') AND hstlog_tbl_delete.relname ~~ '%_delete'::text) unnamed_subquery
  WHERE NULLIF(bronze_schema_name, '"".""'::text) IS NOT NULL AND NULLIF(raw_schema_name, '"".""'::text) IS NOT NULL AND NULLIF(hstlog_schema_name, '"".""'::text) IS NOT NULL AND NULLIF(bronze_path, '"".""'::text) IS NOT NULL AND NULLIF(raw_path, '"".""'::text) IS NOT NULL AND NULLIF(hstlog_path, '"".""'::text) IS NOT NULL AND NULLIF(hstlog_insert_path, '"".""'::text) IS NOT NULL AND NULLIF(hstlog_update_path, '"".""'::text) IS NOT NULL AND NULLIF(hstlog_delete_path, '"".""'::text) IS NOT NULL;

ALTER TABLE data_catalog.vw_catalog
    OWNER TO postgres;
COMMENT ON VIEW data_catalog.vw_catalog
    IS 'View responsável por entregar a relação completa entre todos os objetos do catálogo lógico de dados, suas relações, seus status, suas descrições e demais campos específicos de cada tabela de objetos';

COMMENT ON COLUMN data_catalog.vw_catalog.database_id
    IS 'Coluna referente ao ID do database (data_catalog.tb_databases.id)';

COMMENT ON COLUMN data_catalog.vw_catalog.database_name
    IS 'Coluna referente ao NOME do database (data_catalog.tb_databases.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.database_description
    IS 'Coluna referente a DESCRIÇÃO do database (data_catalog.tb_databases.description)';

COMMENT ON COLUMN data_catalog.vw_catalog.database_status_id
    IS 'Coluna referente ao ID do status do database (data_catalog.tb_databases.tb_status_id)';

COMMENT ON COLUMN data_catalog.vw_catalog.database_status_name
    IS 'Coluna referente ao NOME do status do database (data_catalog.tb_status.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.database_active
    IS 'Coluna referente à SITUAÇÃO ATIVA do database no catálogo. True para ativo e False para inativo';

COMMENT ON COLUMN data_catalog.vw_catalog.schema_id
    IS 'Coluna referente ao ID do schema (data_catalog.tb_schemas.id)';

COMMENT ON COLUMN data_catalog.vw_catalog.schema_name
    IS 'Coluna referente ao NOME do schema (data_catalog.tb_schemas.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.schema_description
    IS 'Coluna referente a DESCRIÇÃO do schema (data_catalog.tb_schemas.description)';

COMMENT ON COLUMN data_catalog.vw_catalog.schema_status_id
    IS 'Coluna referente ao ID do status do schema (data_catalog.tb_schemas.tb_status_id)';

COMMENT ON COLUMN data_catalog.vw_catalog.schema_status_name
    IS 'Coluna referente ao NOME do status do schema (data_catalog.tb_satus.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.schema_active
    IS 'Coluna referente à SITUAÇÃO ATIVA do schema no catálogo. True para ativo e False para inativo';

COMMENT ON COLUMN data_catalog.vw_catalog.table_id
    IS 'Coluna referente ao ID da tabela (data_catalog.tb_tables.id)';

COMMENT ON COLUMN data_catalog.vw_catalog.table_name
    IS 'Coluna referente ao NOME da tabela (data_catalog.tb_tables.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.table_description
    IS 'Coluna referente a DESCRIÇÃO da tabela (data_catalog.tb_tables.description)';

COMMENT ON COLUMN data_catalog.vw_catalog.table_status_id
    IS 'Coluna referente ao ID do status da tabela (data_catalog.tb_tables.tb_status_id)';

COMMENT ON COLUMN data_catalog.vw_catalog.table_status_name
    IS 'Coluna referente ao NOME do status da tabela (data_catalog.tb_status.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.table_payload_period_id
    IS 'Coluna referente ao ID da tabela de payload (periodicidade de carga para camada bronze) da tabela (data_catalog.tb_tables.tb_payload_period_id)';

COMMENT ON COLUMN data_catalog.vw_catalog.table_payload_period_name
    IS 'Coluna referente ao NOME do período que controla a periodicidade de carga da tabela para a camada bronze';

COMMENT ON COLUMN data_catalog.vw_catalog.table_payload_period_minutes
    IS 'Coluna referente aos MINUTOS do período que controla a periodicidade de carga da tabela para a camada bronze';

COMMENT ON COLUMN data_catalog.vw_catalog.table_active
    IS 'Coluna referente à SITUAÇÃO ATIVA da tabela no catálogo. True para ativo e False para inativo';

COMMENT ON COLUMN data_catalog.vw_catalog.column_id
    IS 'Coluna referente ao ID da coluna (data_catalog.tb_columns.id)';

COMMENT ON COLUMN data_catalog.vw_catalog.column_name
    IS 'Coluna referente ao NOME da coluna (data_catalog.tb_columns.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.column_data_type
    IS 'Coluna referente ao TIPO DE DADO da coluna (data_catalog.tb_columns.data_type e data_catalog.tb_data_types.data_type)';

COMMENT ON COLUMN data_catalog.vw_catalog.column_pk
    IS 'Coluna referente à DEFINIÇÃO DE PRIMARY KEY da coluna (data_catalog.tb_columns.is_pk). True para ser uma primary key e False para ser uma coluna convencional';

COMMENT ON COLUMN data_catalog.vw_catalog.column_description
    IS 'Coluna referente a DESCRIÇÃO da coluna (data_catalog.tb_columns.description)';

COMMENT ON COLUMN data_catalog.vw_catalog.column_status_id
    IS 'Coluna referente ao ID do status da coluna (data_catalog.tb_columns.tb_status_id)';

COMMENT ON COLUMN data_catalog.vw_catalog.column_status_name
    IS 'Coluna referente ao NOME do status da coluna (data_catalog.tb_status.name)';

COMMENT ON COLUMN data_catalog.vw_catalog.column_active
    IS 'Coluna referente à SITUAÇÃO ATIVA da coluna. True para ativo e False para inativo';

COMMENT ON COLUMN data_catalog.vw_catalog.bronze_schema_name
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o nome do schema na camada BRONZE.';

COMMENT ON COLUMN data_catalog.vw_catalog.raw_schema_name
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o nome do schema na camada RAW.';

COMMENT ON COLUMN data_catalog.vw_catalog.hstlog_schema_name
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o nome do schema na camada HSTLOG (histórico/auditoria).';

COMMENT ON COLUMN data_catalog.vw_catalog.bronze_path
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o caminho completo contendo nome do schema + nome da tabela na camada BRONZE.';

COMMENT ON COLUMN data_catalog.vw_catalog.raw_path
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o caminho completo contendo nome do schema + nome da tabela na camada RAW.';

COMMENT ON COLUMN data_catalog.vw_catalog.hstlog_path
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o caminho completo contendo nome do schema + nome da tabela na camada HSTLOG (histórico/auditoria).';

COMMENT ON COLUMN data_catalog.vw_catalog.hstlog_insert_path
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o caminho completo contendo nome do schema + nome da tabela particionada para operações de INSERTS na camada HSTLOG (histórico/auditoria).';

COMMENT ON COLUMN data_catalog.vw_catalog.hstlog_update_path
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o caminho completo contendo nome do schema + nome da tabela particionada para operações de UPDATES na camada HSTLOG (histórico/auditoria).';

COMMENT ON COLUMN data_catalog.vw_catalog.hstlog_delete_path
    IS 'Coluna já preparada e tratada com aspas duplas para prevenir objetos com nomes case sensitive e/ou fora do padrão, com hífens, espaços e demais circunstâncias. Nesta coluna, é armazenado o caminho completo contendo nome do schema + nome da tabela particionada para operações de DELETES na camada HSTLOG (histórico/auditoria).';