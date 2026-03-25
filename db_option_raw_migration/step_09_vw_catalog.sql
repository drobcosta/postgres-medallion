-- View: data_catalog.vw_catalog

-- DROP VIEW data_catalog.vw_catalog;

CREATE OR REPLACE VIEW data_catalog.vw_catalog
 AS
 SELECT d.id AS database_id,
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
    c.active AS column_active
   FROM data_catalog.tb_databases d
     LEFT JOIN data_catalog.tb_status d_st ON d_st.id = d.tb_status_id
     LEFT JOIN data_catalog.tb_schemas s ON s.tb_databases_id::text = d.id::text
     LEFT JOIN data_catalog.tb_status s_st ON s_st.id = s.tb_status_id
     LEFT JOIN data_catalog.tb_tables t ON t.tb_databases_id::text = d.id::text AND t.tb_schemas_id::text = s.id::text
     LEFT JOIN data_catalog.tb_status t_st ON t_st.id = t.tb_status_id
     LEFT JOIN data_catalog.tb_payload_period t_p ON t_p.id = t.tb_payload_period_id
     LEFT JOIN data_catalog.tb_columns c ON c.tb_databases_id::text = d.id::text AND c.tb_schemas_id::text = s.id::text AND c.tb_tables_id::text = t.id::text
     LEFT JOIN data_catalog.tb_status c_st ON c_st.id = c.tb_status_id;

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

