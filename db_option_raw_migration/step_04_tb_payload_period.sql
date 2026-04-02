CREATE SEQUENCE IF NOT EXISTS data_catalog.tb_payload_period_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

CREATE TABLE IF NOT EXISTS data_catalog.tb_payload_period
(
    id integer NOT NULL DEFAULT nextval('data_catalog.tb_payload_period_id_seq'::regclass),
    name character varying(100) COLLATE pg_catalog."default" NOT NULL,
    minutes integer NOT NULL DEFAULT 10,
    created_at timestamp without time zone NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tb_payload_period_pkey PRIMARY KEY (id)
);

COMMENT ON TABLE data_catalog.tb_payload_period
    IS 'Tabela responsável pelos registros dos períodos de execução de carga (payload) das tabelas disponíveis na camada bronze. São períodos pré-configurados para que a carga de dados da camada raw para a camada bronze aconteça.';

COMMENT ON COLUMN data_catalog.tb_payload_period.id
    IS 'Coluna PK da tabela';

COMMENT ON COLUMN data_catalog.tb_payload_period.name
    IS 'Coluna que registra o nome do período de execução de carga (payload)';

COMMENT ON COLUMN data_catalog.tb_payload_period.minutes
    IS 'Coluna que registra a quantidade de minutos do período de execução de carga';

COMMENT ON COLUMN data_catalog.tb_payload_period.created_at
    IS 'Coluna que registra o timestamp exato da inserção do registro na tabela';