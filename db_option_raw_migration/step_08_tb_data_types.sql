CREATE TABLE IF NOT EXISTS data_catalog.tb_data_types
(
    data_type character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT tb_data_types_pk PRIMARY KEY (data_type)
);

COMMENT ON TABLE data_catalog.tb_data_types
    IS 'Tabela responsável por armazenar os data types das colunas que usaremos no processo de DE x PARA e padronização dos data types dos dados da camada raw na camada bronze';

COMMENT ON COLUMN data_catalog.tb_data_types.data_type
    IS 'Coluna PK da tabela. Seu conteúdo refere-se a data type de colunas';