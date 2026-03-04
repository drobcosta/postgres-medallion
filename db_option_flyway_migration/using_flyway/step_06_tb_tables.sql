CREATE TABLE IF NOT EXISTS data_catalog.tb_tables
(
    id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_databases_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_schemas_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_status_id integer NOT NULL,
    tb_payload_period_id integer,
    name character varying(250) COLLATE pg_catalog."default" NOT NULL,
    description character varying(250) COLLATE pg_catalog."default",
    active boolean NOT NULL DEFAULT true,
    created_at timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamp without time zone,
    CONSTRAINT tb_tables_pk PRIMARY KEY (id, tb_databases_id, tb_schemas_id),
    CONSTRAINT tb_tables_tb_payload_period_id_fkey FOREIGN KEY (tb_payload_period_id)
        REFERENCES data_catalog.tb_payload_period (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT tb_tables_tb_status_id_fkey FOREIGN KEY (tb_status_id)
        REFERENCES data_catalog.tb_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT tb_tables_to_tb_schemas_fk FOREIGN KEY (tb_databases_id, tb_schemas_id)
        REFERENCES data_catalog.tb_schemas (tb_databases_id, id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
) TABLESPACE pg_default;

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
