COMMENT ON DATABASE bronze_db IS 'Database com raw data (origem change data capture) e camada bronze (origem function data_catalog.bronze_layer)';

CREATE SCHEMA IF NOT EXISTS data_catalog AUTHORIZATION adm_rds_dp;
COMMENT ON SCHEMA data_catalog IS 'Schema responsável pelo catálogo lógico de dados';

CREATE TABLE IF NOT EXISTS data_catalog.tb_status (
	id SERIAL PRIMARY KEY,
	"name" VARCHAR(100) NOT NULL,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT clock_timestamp()
);
COMMENT ON TABLE data_catalog.tb_status IS 'Tabela responsável pelos status de cada objeto dentro do catálogo lógico de dados';
COMMENT ON COLUMN data_catalog.tb_status.id IS 'Coluna PK da tabela';
COMMENT ON COLUMN data_catalog.tb_status.name IS 'Coluna que registra o nome do status';
COMMENT ON COLUMN data_catalog.tb_status.created_at IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';

CREATE TABLE IF NOT EXISTS data_catalog.tb_payload_period (
	id SERIAL PRIMARY KEY,
	"name" VARCHAR(100) NOT NULL,
	minutes INTEGER NOT NULL DEFAULT 10,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT clock_timestamp()
);
COMMENT ON TABLE data_catalog.tb_payload_period IS 'Tabela responsável pelos registros dos períodos de execução de carga (payload) das tabelas disponíveis na camada bronze. São períodos pré-configurados para que a carga de dados da camada raw para a camada bronze aconteça.'
COMMENT ON COLUMN data_catalog.tb_payload_period.id IS 'Coluna PK da tabela';
COMMENT ON COLUMN data_catalog.tb_payload_period.name IS 'Coluna que registra o nome do período de execução de carga (payload)';
COMMENT ON COLUMN data_catalog.tb_payload_period.minutes IS 'Coluna que registra a quantidade de minutos do período de execução de carga';
COMMENT ON COLUMN data_catalog.tb_payload_period.created_at IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';

CREATE TABLE IF NOT EXISTS data_catalog.tb_databases (
	id VARCHAR(32) NOT NULL,
	tb_status_id INTEGER NOT NULL REFERENCES data_catalog.tb_status(id),
	"name" VARCHAR(250) NOT NULL,
	description VARCHAR(250),
	active BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITHOUT TIME ZONE,
	CONSTRAINT tb_databases_pk PRIMARY KEY(id)
);
COMMENT ON TABLE data_catalog.tb_databases IS 'Tabela responsável por armazenar os nomes dos bancos de dados que deverão ser registrados na camada bronze';
COMMENT ON COLUMN data_catalog.tb_databases.id IS 'Coluna PK da tabela. Seu conteúdo é um MD5 da coluna NAME';
COMMENT ON COLUMN data_catalog.tb_databases.tb_status_id IS 'Coluna FK referência em data_catalog.tb_status(id). Registra o status do objeto (database)';
COMMENT ON COLUMN data_catalog.tb_databases.name IS 'Coluna que registra o nome do objeto (database)';
COMMENT ON COLUMN data_catalog.tb_databases.description IS 'Coluna que registra a descrição do objeto (database). Esta coluna inicialmente é NULL. Esta coluna é obrigatória para que o objeto seja criado na camada bronze';
COMMENT ON COLUMN data_catalog.tb_databases.active IS 'Coluna que registra se o objeto (database) está ativo no catálogo';
COMMENT ON COLUMN data_catalog.tb_databases.created_at IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';
COMMENT ON COLUMN data_catalog.tb_databases.updated_at IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela';

CREATE TABLE IF NOT EXISTS data_catalog.tb_schemas (
	id VARCHAR(32) NOT NULL,
	tb_databases_id VARCHAR(32) NOT NULL REFERENCES data_catalog.tb_databases(id),
	tb_status_id INTEGER NOT NULL REFERENCES data_catalog.tb_status(id),
	"name" VARCHAR(250) NOT NULL,
	description VARCHAR(250),
	active BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITHOUT TIME ZONE,
	CONSTRAINT tb_schemas_pk PRIMARY KEY(id, tb_databases_id)
);

COMMENT ON TABLE data_catalog.tb_schemas IS 'Tabela responsável por armazenar os nomes dos schemas vinculados aos bancos de dados que deverão ser registrados na camada bronze';
COMMENT ON COLUMN data_catalog.tb_schemas.id IS 'Coluna PK da tabela. Seu conteúdo é um MD5 da coluna NAME';
COMMENT ON COLUMN data_catalog.tb_schemas.tb_databases_id IS 'Coluna FK referência em data_catalog.tb_databases(id). Registra a qual database este schema pertence'
COMMENT ON COLUMN data_catalog.tb_schemas.tb_status_id IS 'Coluna FK referência em data_catalog.tb_status(id). Registra o status do objeto (schema)'
COMMENT ON COLUMN data_catalog.tb_schemas.name IS 'Coluna que registra o nome do objeto (schema)'
COMMENT ON COLUMN data_catalog.tb_schemas.description IS 'Coluna que registra a descrição do objeto (schema). Esta coluna inicialmente é NULL. Esta coluna é obrigatória para que o objeto seja criado na camada bronze'
COMMENT ON COLUMN data_catalog.tb_schemas.active IS 'Coluna que registra se o objeto (schema) está ativo no catálogo'
COMMENT ON COLUMN data_catalog.tb_schemas.created_at IS 'Coluna que registra o timestamp exato da inserção do registro na tabela'
COMMENT ON COLUMN data_catalog.tb_schemas.updated_at IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela'

CREATE TABLE IF NOT EXISTS data_catalog.tb_tables (
	id VARCHAR(32) NOT NULL,
	tb_databases_id VARCHAR(32) NOT NULL,
	tb_schemas_id VARCHAR(32) NOT NULL,
	tb_status_id INTEGER NOT NULL REFERENCES data_catalog.tb_status(id),
	tb_payload_period_id INTEGER NULL REFERENCES data_catalog.tb_payload_period(id),
	"name" VARCHAR(250) NOT NULL,
	description VARCHAR(250),
	active BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITHOUT TIME ZONE,
	CONSTRAINT tb_tables_pk PRIMARY KEY(id, tb_databases_id, tb_schemas_id),
	CONSTRAINT tb_tables_to_tb_schemas_fk FOREIGN KEY (tb_databases_id, tb_schemas_id) REFERENCES data_catalog.tb_schemas(tb_databases_id, id)
);
COMMENT ON TABLE data_catalog.tb_tables IS 'Tabela responsável por armazenar os nomes das tabelas vinculadas aos schemas e aos bancos de dados que deverão ser registrados na camada bronze';
COMMENT ON COLUMNS data_catalog.tb_tables.id IS 'Coluna PK da tabela. Seu conteúdo é um MD5 da coluna NAME';
COMMENT ON COLUMNS data_catalog.tb_tables.tb_databases_id IS 'Coluna FK referência em data_catalog.tb_databases(id). Registra a qual database esta tabela pertence';
COMMENT ON COLUMNS data_catalog.tb_tables.tb_schemas_id IS 'Coluna FK referência em data_catalog.tb_schemas(id). Registra a qual schema esta tabela pertence';
COMMENT ON COLUMNS data_catalog.tb_tables.tb_status_id IS 'Coluna FK referência em data_catalog.tb_status(id). Registra o status do objeto (table)';
COMMENT ON COLUMNS data_catalog.tb_tables.tb_payload_period_id IS 'Coluna FK referência em data_catalog.tb_payload_period(id). Registra qual a periodicidade da carga (payload) entre camada raw e bronze deverá acontecer';
COMMENT ON COLUMNS data_catalog.tb_tables.name IS 'Coluna que registra o nome do objeto (table)';
COMMENT ON COLUMNS data_catalog.tb_tables.description IS 'Coluna que registra a descrição do objeto (table). Esta coluna inicialmente é NULL. Esta coluna é obrigatória para que o objeto seja criado na camada bronze';
COMMENT ON COLUMNS data_catalog.tb_tables.active IS 'Coluna que registra se o objeto (table) está ativo no catálogo';
COMMENT ON COLUMNS data_catalog.tb_tables.created_at IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';
COMMENT ON COLUMNS data_catalog.tb_tables.updated_at IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela';

CREATE TABLE IF NOT EXISTS data_catalog.tb_data_types (
	data_type VARCHAR NOT NULL,
	CONSTRAINT tb_data_types_pk PRIMARY KEY (data_type)
);

COMMENT ON TABLE data_catalog.tb_data_types IS 'Tabela responsável por armazenar os data types das colunas que usaremos no processo de DE x PARA e padronização dos data types dos dados da camada raw na camada bronze';
COMMENT ON COLUMN data_catalog.tb_data_types.data_type IS 'Coluna PK da tabela. Seu conteúdo refere-se a data type de colunas';

CREATE TABLE IF NOT EXISTS data_catalog.tb_columns (
	id VARCHAR(32) NOT NULL,
	tb_databases_id VARCHAR(32) NOT NULL,
	tb_schemas_id VARCHAR(32) NOT NULL,
	tb_tables_id VARCHAR(32) NOT NULL,
	tb_status_id INTEGER NOT NULL REFERENCES data_catalog.tb_status(id),
	"name" VARCHAR(250) NOT NULL,
	description VARCHAR(250),
	data_type VARCHAR(100) NULL,
	is_pk BOOLEAN NOT NULL DEFAULT FALSE,
	active BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITHOUT TIME ZONE,
	CONSTRAINT tb_columns_pk PRIMARY KEY(id, tb_databases_id, tb_schemas_id, tb_tables_id),
	CONSTRAINT tb_columns_to_tb_tables_fk FOREIGN KEY(tb_databases_id,tb_schemas_id,tb_tables_id) REFERENCES data_catalog.tb_tables(tb_databases_id,tb_schemas_id,id),
	CONSTRAINT tb_columns_to_tb_data_types_fk FOREIGN KEY(data_type) REFERENCES data_catalog.tb_data_types(data_type)
);

COMMENT ON TABLE data_catalog.tb_columns IS 'Tabela responsável por armazenar os nomes das colunas das tabelas vinculadas aos schemas e aos bancos de dados que deverão ser registrados na camada bronze';
COMMENT ON COLUMN data_catalog.tb_columns.id IS 'Coluna PK da tabela. Seu conteúdo é um MD5 da coluna NAME';
COMMENT ON COLUMN data_catalog.tb_columns.tb_databases_id IS 'Coluna FK referência em data_catalog.tb_databases(id). Registra a qual database esta coluna pertence';
COMMENT ON COLUMN data_catalog.tb_columns.tb_schemas_id IS 'Coluna FK referência em data_catalog.tb_schemas(id). Registra a qual schema esta coluna pertence';
COMMENT ON COLUMN data_catalog.tb_columns.tb_tables_id IS 'Coluna FK referência em data_catalog.tb_tables(id). Registra a qual tabela esta coluna pertence';
COMMENT ON COLUMN data_catalog.tb_columns.tb_status_id IS 'Coluna FK referência em data_catalog.tb_status(id). Registra o status do objeto (column)';
COMMENT ON COLUMN data_catalog.tb_columns.name IS 'Coluna que registra o nome do objeto (column)';
COMMENT ON COLUMN data_catalog.tb_columns.description IS 'Coluna que registra a descrição do objeto (table). Esta coluna inicialmente é NULL. Esta coluna é obrigatória para que o objeto seja criado na camada bronze';
COMMENT ON COLUMN data_catalog.tb_columns.data_type IS 'Coluna FK referência em data_catalog.tb_data_types(id). Registra qual é o tipo de dado desta coluna para criação da mesma na camada bronze';
COMMENT ON COLUMN data_catalog.tb_columns.is_pk IS 'Coluna que indica se o registro (column) é considerado uma primary key para a tabela (este dado é puxado automaticamente da camada raw proveniente do CDC DMS';
COMMENT ON COLUMN data_catalog.tb_columns.active IS 'Coluna que registra se o objeto (table) está ativo no catálogo';
COMMENT ON COLUMN data_catalog.tb_columns.created_at IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';
COMMENT ON COLUMN data_catalog.tb_columns.updated_at IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela';

CREATE OR REPLACE VIEW data_catalog.vw_catalog AS
SELECT	d.id AS database_id
		, d.name AS database_name
		, d.description AS database_description
		, d.tb_status_id AS database_status_id
		, d_st.name AS database_status_name
		, d.active AS database_active
		, s.id AS schema_id
		, s.name AS schema_name
		, s.description AS schema_description
		, s.tb_status_id AS schema_status_id
		, s_st.name AS schema_status_name
		, s.active AS schema_active
		, t.id AS table_id
		, t.name AS table_name
		, t.description AS table_description
		, t.tb_status_id AS table_status_id
		, t_st.name AS table_status_name
		, t.tb_payload_period_id AS table_payload_period_id
		, t_p.name AS table_payload_period_name
		, t.active AS table_active
		, c.id AS column_id
		, c.name AS column_name
		, dt.data_type AS column_data_type
		, c.is_pk AS column_pk
		, c.description AS column_description
		, c.tb_status_id AS column_status_id
		, c_st.name AS column_status_name
		, c.active AS column_active
FROM data_catalog.tb_databases d
LEFT JOIN data_catalog.tb_status d_st
	ON d_st.id = d.tb_status_id
LEFT JOIN data_catalog.tb_schemas s
	ON s.tb_databases_id = d.id
LEFT JOIN data_catalog.tb_status s_st
	ON s_st.id = s.tb_status_id
LEFT JOIN data_catalog.tb_tables t
	ON t.tb_databases_id = d.id
	AND t.tb_schemas_id = s.id
LEFT JOIN data_catalog.tb_status t_st
	ON t_st.id = t.tb_status_id
LEFT JOIN data_catalog.tb_payload_period t_p
	ON t_p.id = t.tb_payload_period_id
LEFT JOIN data_catalog.tb_columns c
	ON c.tb_databases_id = d.id
	AND c.tb_schemas_id = s.id
	AND c.tb_tables_id = t.id
LEFT JOIN data_catalog.tb_data_types dt
	ON dt.data_type = c.data_type
LEFT JOIN data_catalog.tb_status c_st
	ON c_st.id = c.tb_status_id;

COMMENT ON VIEW data_catalog.vw_catalog IS 'View responsável por entregar a relação completa entre todos os objetos do catálogo lógico de dados, suas relações, seus status, suas descrições e demais campos específicos de cada tabela de objetos';
COMMENT ON COLUMN data_catalog.database_id IS 'Coluna referente ao ID do database (data_catalog.tb_databases.id)';
COMMENT ON COLUMN data_catalog.database_name IS 'Coluna referente ao NOME do database (data_catalog.tb_databases.name)';
COMMENT ON COLUMN data_catalog.database_description IS 'Coluna referente a DESCRIÇÃO do database (data_catalog.tb_databases.description)';
COMMENT ON COLUMN data_catalog.database_status_id IS 'Coluna referente ao ID do status do database (data_catalog.tb_databases.tb_status_id)';
COMMENT ON COLUMN data_catalog.database_status_name IS 'Coluna referente ao NOME do status do database (data_catalog.tb_status.name)';
COMMENT ON COLUMN data_catalog.database_active IS 'Coluna referente à SITUAÇÃO ATIVA do database no catálogo. True para ativo e False para inativo';
COMMENT ON COLUMN data_catalog.schema_id IS 'Coluna referente ao ID do schema (data_catalog.tb_schemas.id)';
COMMENT ON COLUMN data_catalog.schema_name IS 'Coluna referente ao NOME do schema (data_catalog.tb_schemas.name)';
COMMENT ON COLUMN data_catalog.schema_description IS 'Coluna referente a DESCRIÇÃO do schema (data_catalog.tb_schemas.description)';
COMMENT ON COLUMN data_catalog.schema_status_id IS 'Coluna referente ao ID do status do schema (data_catalog.tb_schemas.tb_status_id)';
COMMENT ON COLUMN data_catalog.schema_status_name IS 'Coluna referente ao NOME do status do schema (data_catalog.tb_satus.name)';
COMMENT ON COLUMN data_catalog.schema_active IS 'Coluna referente à SITUAÇÃO ATIVA do schema no catálogo. True para ativo e False para inativo';
COMMENT ON COLUMN data_catalog.table_id IS 'Coluna referente ao ID da tabela (data_catalog.tb_tables.id)';
COMMENT ON COLUMN data_catalog.table_name IS 'Coluna referente ao NOME da tabela (data_catalog.tb_tables.name)';
COMMENT ON COLUMN data_catalog.table_description IS 'Coluna referente a DESCRIÇÃO da tabela (data_catalog.tb_tables.description)';
COMMENT ON COLUMN data_catalog.table_status_id IS 'Coluna referente ao ID do status da tabela (data_catalog.tb_tables.tb_status_id)';
COMMENT ON COLUMN data_catalog.table_status_name IS 'Coluna referente ao NOME do status da tabela (data_catalog.tb_status.name)';
COMMENT ON COLUMN data_catalog.table_payload_period_id IS 'Coluna referente ao ID da tabela de payload (periodicidade de carga para camada bronze) da tabela (data_catalog.tb_tables.tb_payload_period_id)';
COMMENT ON COLUMN data_catalog.table_payload_period_name IS 'Coluna referente ao NOME do período que controla a periodicidade de carga da tabela para a camada bronze';
COMMENT ON COLUMN data_catalog.table_active IS 'Coluna referente à SITUAÇÃO ATIVA da tabela no catálogo. True para ativo e False para inativo';
COMMENT ON COLUMN data_catalog.column_id IS 'Coluna referente ao ID da coluna (data_catalog.tb_columns.id)';
COMMENT ON COLUMN data_catalog.column_name IS 'Coluna referente ao NOME da coluna (data_catalog.tb_columns.name)';
COMMENT ON COLUMN data_catalog.column_data_type IS 'Coluna referente ao TIPO DE DADO da coluna (data_catalog.tb_columns.data_type e data_catalog.tb_data_types.data_type)';
COMMENT ON COLUMN data_catalog.column_pk IS 'Coluna referente à DEFINIÇÃO DE PRIMARY KEY da coluna (data_catalog.tb_columns.is_pk). True para ser uma primary key e False para ser uma coluna convencional';
COMMENT ON COLUMN data_catalog.column_description IS 'Coluna referente a DESCRIÇÃO da coluna (data_catalog.tb_columns.description)';
COMMENT ON COLUMN data_catalog.column_status_id IS 'Coluna referente ao ID do status da coluna (data_catalog.tb_columns.tb_status_id)';
COMMENT ON COLUMN data_catalog.column_status_name IS 'Coluna referente ao NOME do status da coluna (data_catalog.tb_status.name)';
COMMENT ON COLUMN data_catalog.column_active IS 'Coluna referente à SITUAÇÃO ATIVA da coluna. True para ativo e False para inativo';
