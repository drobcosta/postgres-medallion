CREATE SEQUENCE IF NOT EXISTS data_catalog.tb_status_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

CREATE TABLE IF NOT EXISTS data_catalog.tb_status
(
    id integer NOT NULL DEFAULT nextval('data_catalog.tb_status_id_seq'::regclass),
    name character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tb_status_pkey PRIMARY KEY (id)
);

COMMENT ON TABLE data_catalog.tb_status
    IS 'Tabela responsável pelos status de cada objeto dentro do catálogo lógico de dados';

COMMENT ON COLUMN data_catalog.tb_status.id
    IS 'Coluna PK da tabela';

COMMENT ON COLUMN data_catalog.tb_status.name
    IS 'Coluna que registra o nome do status';

COMMENT ON COLUMN data_catalog.tb_status.created_at
    IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';