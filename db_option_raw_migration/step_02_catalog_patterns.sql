CREATE SEQUENCE IF NOT EXISTS data_catalog.tb_raw_patterns_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

CREATE TABLE IF NOT EXISTS data_catalog.tb_raw_patterns
(
    id integer NOT NULL DEFAULT nextval('data_catalog.tb_raw_patterns_id_seq'::regclass),
    pattern character varying COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone,
    CONSTRAINT tb_raw_patterns_pkey PRIMARY KEY (id),
    CONSTRAINT tb_raw_patterns_pattern_key UNIQUE (pattern)
);

COMMENT ON TABLE data_catalog.tb_raw_patterns
    IS 'Tabela responsável por armazenar os sufixos dos schemas da camada raw que deverão ser considerados elegíveis para iniciarem no processos de CDC entre camada RAW e camada BRONZE como também pertencerem ao catálogo lógico de dados.';

COMMENT ON COLUMN data_catalog.tb_raw_patterns.id
    IS 'Coluna PK da tabela.';

COMMENT ON COLUMN data_catalog.tb_raw_patterns.pattern
    IS 'Coluna com o pattern a ser considerado dos objetos na camada raw';

COMMENT ON COLUMN data_catalog.tb_raw_patterns.created_at
    IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';

COMMENT ON COLUMN data_catalog.tb_raw_patterns.updated_at
    IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela';

CREATE SEQUENCE IF NOT EXISTS data_catalog.tb_raw_databases_schemas_excluded_patterns_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

CREATE TABLE IF NOT EXISTS data_catalog.tb_raw_databases_schemas_excluded_patterns
(
    id integer NOT NULL DEFAULT nextval('data_catalog.tb_raw_databases_schemas_excluded_patterns_id_seq'::regclass),
    pattern character varying COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone,
    CONSTRAINT tb_raw_databases_schemas_excluded_patterns_pkey PRIMARY KEY (id),
    CONSTRAINT tb_raw_databases_schemas_excluded_patterns_pattern_key UNIQUE (pattern)
); 

COMMENT ON TABLE data_catalog.tb_raw_databases_schemas_excluded_patterns
    IS 'Tabela responsável por armazenar as palavras padrões que deverão ser desconsideradas dos schemas da camada RAW para a formação do nome dos schemas na camada BRONZE. Por exemplo, se um schema na camada RAW chamar users_db_public_raw e os patterns armazenados nesta tabela forem _public e _raw, o nome do novo schema na camada BRONZE, desconsiderando os padrões, deverá ser users_db.';

COMMENT ON COLUMN data_catalog.tb_raw_databases_schemas_excluded_patterns.id
    IS 'Coluna PK da tabela.';

COMMENT ON COLUMN data_catalog.tb_raw_databases_schemas_excluded_patterns.pattern
    IS 'Coluna com o pattern a ser desconsiderado dos objetos na camada raw';

COMMENT ON COLUMN data_catalog.tb_raw_databases_schemas_excluded_patterns.created_at
    IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';

COMMENT ON COLUMN data_catalog.tb_raw_databases_schemas_excluded_patterns.updated_at
    IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela';