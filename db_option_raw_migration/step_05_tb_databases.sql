CREATE TABLE IF NOT EXISTS data_catalog.tb_databases
(
    id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    name character varying(250) COLLATE pg_catalog."default" NOT NULL,
    description character varying(250) COLLATE pg_catalog."default",
    tb_status_id integer NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamp without time zone,
    CONSTRAINT tb_databases_pk PRIMARY KEY (id),
    CONSTRAINT tb_databases_tb_status_id_fkey FOREIGN KEY (tb_status_id)
        REFERENCES data_catalog.tb_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

COMMENT ON TABLE data_catalog.tb_databases
    IS 'Tabela responsável por armazenar os nomes dos bancos de dados que deverão ser registrados na camada bronze';

COMMENT ON COLUMN data_catalog.tb_databases.id
    IS 'Coluna PK da tabela. Seu conteúdo é um MD5 da coluna NAME';

COMMENT ON COLUMN data_catalog.tb_databases.name
    IS 'Coluna que registra o nome do objeto (database)';

COMMENT ON COLUMN data_catalog.tb_databases.description
    IS 'Coluna que registra a descrição do objeto (database). Esta coluna inicialmente é NULL. Esta coluna é obrigatória para que o objeto seja criado na camada bronze';

COMMENT ON COLUMN data_catalog.tb_databases.tb_status_id
    IS 'Coluna FK referência em data_catalog.tb_status(id). Registra o status do objeto (database)';

COMMENT ON COLUMN data_catalog.tb_databases.active
    IS 'Coluna que registra se o objeto (database) está ativo no catálogo';

COMMENT ON COLUMN data_catalog.tb_databases.created_at
    IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';

COMMENT ON COLUMN data_catalog.tb_databases.updated_at
    IS 'Coluna que registra o timestamp exato da alteração da coluna tb_status_id do registro na tabela';