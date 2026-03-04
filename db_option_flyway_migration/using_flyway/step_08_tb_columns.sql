CREATE TABLE IF NOT EXISTS data_catalog.tb_columns
(
    id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_databases_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_schemas_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_tables_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_status_id integer NOT NULL,
    name character varying(250) COLLATE pg_catalog."default" NOT NULL,
    description character varying(250) COLLATE pg_catalog."default",
    data_type character varying(100) COLLATE pg_catalog."default",
    is_pk boolean NOT NULL DEFAULT false,
    active boolean NOT NULL DEFAULT true,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone,
    CONSTRAINT tb_columns_pk PRIMARY KEY (id, tb_databases_id, tb_schemas_id, tb_tables_id),
    CONSTRAINT tb_columns_tb_status_id_fkey FOREIGN KEY (tb_status_id)
        REFERENCES data_catalog.tb_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT tb_columns_to_tb_data_types_fk FOREIGN KEY (data_type)
        REFERENCES data_catalog.tb_data_types (data_type) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT tb_columns_to_tb_tables_fk FOREIGN KEY (tb_databases_id, tb_schemas_id, tb_tables_id)
        REFERENCES data_catalog.tb_tables (tb_databases_id, tb_schemas_id, id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
) TABLESPACE pg_default;

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
