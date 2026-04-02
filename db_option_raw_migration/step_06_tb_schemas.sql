CREATE TABLE IF NOT EXISTS data_catalog.tb_schemas
(
    id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_databases_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    tb_status_id integer NOT NULL,
    name character varying(250) COLLATE pg_catalog."default" NOT NULL,
    description character varying(250) COLLATE pg_catalog."default",
    active boolean NOT NULL DEFAULT true,
    created_at timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamp without time zone,
    CONSTRAINT tb_schemas_pk PRIMARY KEY (id, tb_databases_id),
    CONSTRAINT tb_schemas_tb_databases_id_fkey FOREIGN KEY (tb_databases_id)
        REFERENCES data_catalog.tb_databases (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT tb_schemas_tb_status_id_fkey FOREIGN KEY (tb_status_id)
        REFERENCES data_catalog.tb_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

COMMENT ON TABLE data_catalog.tb_schemas
    IS 'Tabela responsável por armazenar os nomes dos schemas vinculados aos bancos de dados que deverão ser registrados na camada bronze';

COMMENT ON COLUMN data_catalog.tb_schemas.id
    IS 'Coluna PK da tabela. Seu conteúdo é um MD5 da coluna NAME';

COMMENT ON COLUMN data_catalog.tb_schemas.tb_databases_id
    IS 'Coluna FK referência em data_catalog.tb_databases(id). Registra a qual database este schema pertence';

COMMENT ON COLUMN data_catalog.tb_schemas.tb_status_id
    IS 'Coluna FK referência em data_catalog.tb_status(id). Registra o status do objeto (schema)';

COMMENT ON COLUMN data_catalog.tb_schemas.name
    IS 'Coluna que registra o nome do objeto (schema)';

COMMENT ON COLUMN data_catalog.tb_schemas.description
    IS 'Coluna que registra a descrição do objeto (schema). Esta coluna inicialmente é NULL. Esta coluna é obrigatória para que o objeto seja criado na camada bronze';

COMMENT ON COLUMN data_catalog.tb_schemas.active
    IS 'Coluna que registra se o objeto (schema) está ativo no catálogo';

COMMENT ON COLUMN data_catalog.tb_schemas.created_at
    IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';

COMMENT ON COLUMN data_catalog.tb_schemas.updated_at
    IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela';