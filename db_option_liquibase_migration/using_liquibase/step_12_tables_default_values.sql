INSERT INTO data_catalog.tb_status ("name") VALUES ('NOVO OBJETO (ANALISAR)'),('APROVADO DATA PLATFORM'),('REPROVADO DATA PLATFORM'),('EM CRIAÇÃO DATA PLATFORM'),('DISPONÍVEL DATA PLATFORM'),('APROVADO PARA REMOÇÃO DATA PLATFORM'),('EM REMOÇÃO DATA PLATFORM'),('REMOVIDO DATA PLATFORM');

INSERT INTO data_catalog.tb_payload_period ("name",minutes) VALUES ('Every 5 minutes',5),('Every 10 minutes',10),('Every 30 minutes',30),('Every 1 hour',60),('Every 3 hours',180),('Every 6 hours',360),('Every 12 hours',720),('Every day',1444);

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
