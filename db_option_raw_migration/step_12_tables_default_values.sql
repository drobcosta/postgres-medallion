INSERT INTO data_catalog.tb_status ("name", created_at) VALUES ('NOVO OBJETO (ANALISAR)',clock_timestamp()),('APROVADO DATA PLATFORM',clock_timestamp()),('REPROVADO DATA PLATFORM',clock_timestamp()),('EM CRIAÇÃO DATA PLATFORM',clock_timestamp()),('DISPONÍVEL DATA PLATFORM',clock_timestamp()),('APROVADO PARA REMOÇÃO DATA PLATFORM',clock_timestamp()),('EM REMOÇÃO DATA PLATFORM',clock_timestamp()),('REMOVIDO DATA PLATFORM',clock_timestamp());

INSERT INTO data_catalog.tb_payload_period ("name",minutes,created_at) VALUES ('Every 5 minutes',5,clock_timestamp()),('Every 10 minutes',10,clock_timestamp()),('Every 30 minutes',30,clock_timestamp()),('Every 1 hour',60,clock_timestamp()),('Every 3 hours',180,clock_timestamp()),('Every 6 hours',360,clock_timestamp()),('Every 12 hours',720,clock_timestamp()),('Every day',1444,clock_timestamp());

INSERT INTO data_catalog.tb_data_types VALUES 
('SMALLINT'),
('INTEGER'),
('BIGINT'),
('NUMERIC'),
('NUMERIC(12,2)'),
('DECIMAL'),
('CHAR'),
('VARCHAR'),
('VARCHAR(10)'),
('VARCHAR(50)'),
('VARCHAR(100)'),
('VARCHAR(150)'),
('VARCHAR(200)'),
('VARCHAR(250)'),
('TEXT'),
('DATE'),
('TIMESTAMP'),
('TIMESTAMP WITHOUT TIME ZONE'),
('BOOLEAN'),
('JSON'),
('JSONB'),
('UUID'),
('MONEY'),
('INET')
ON CONFLICT ON CONSTRAINT tb_data_types_pk DO NOTHING;
