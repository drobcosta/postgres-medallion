-- Trigger responsável por validar e restringir as transições de status
-- dos objetos do catálogo lógico na tabela tb_databases.
-- Esta trigger garante que somente transições permitidas no workflow
-- oficial sejam executadas, impedindo saltos indevidos entre status.
CREATE TRIGGER tg_status_object_restriction
BEFORE UPDATE ON data_catalog.tb_databases
FOR EACH ROW WHEN (NEW.tb_status_id IS DISTINCT FROM OLD.tb_status_id)
EXECUTE FUNCTION data_catalog.tg_status_object_restriction();

COMMENT ON TRIGGER tg_status_object_restriction 
ON data_catalog.tb_databases 
IS 'Trigger responsável por validar e restringir as transições de status dos objetos do catálogo lógico na tabela tb_databases, garantindo que apenas mudanças permitidas no workflow sejam executadas.';

-- Trigger responsável por validar e restringir as transições de status
-- dos objetos do catálogo lógico na tabela tb_schemas.
-- Esta trigger garante que o workflow seja respeitado e que o schema
-- só avance para status válidos conforme as regras de governança.
CREATE TRIGGER tg_status_object_restriction
BEFORE UPDATE ON data_catalog.tb_schemas
FOR EACH ROW WHEN (NEW.tb_status_id IS DISTINCT FROM OLD.tb_status_id)
EXECUTE FUNCTION data_catalog.tg_status_object_restriction();

COMMENT ON TRIGGER tg_status_object_restriction 
ON data_catalog.tb_schemas 
IS 'Trigger responsável por validar e restringir as transições de status dos objetos do catálogo lógico na tabela tb_schemas, assegurando que o workflow de governança seja respeitado.';

-- Trigger responsável por validar e restringir as transições de status
-- dos objetos do catálogo lógico na tabela tb_tables.
-- Esta trigger impede que tabelas avancem no workflow sem descrição,
-- periodicidade definida ou aprovação manual quando necessário.
CREATE TRIGGER tg_status_object_restriction
BEFORE UPDATE ON data_catalog.tb_tables
FOR EACH ROW WHEN (NEW.tb_status_id IS DISTINCT FROM OLD.tb_status_id)
EXECUTE FUNCTION data_catalog.tg_status_object_restriction();

COMMENT ON TRIGGER tg_status_object_restriction 
ON data_catalog.tb_tables 
IS 'Trigger responsável por validar e restringir as transições de status dos objetos do catálogo lógico na tabela tb_tables, impedindo avanços sem descrição, periodicidade ou aprovação adequada.';

-- Trigger responsável por validar e restringir as transições de status
-- dos objetos do catálogo lógico na tabela tb_columns.
-- Esta trigger garante que colunas só avancem no workflow quando
-- possuírem descrição, tipo de dado válido e aprovação adequada.
CREATE TRIGGER tg_status_object_restriction
BEFORE UPDATE ON data_catalog.tb_columns
FOR EACH ROW WHEN (NEW.tb_status_id IS DISTINCT FROM OLD.tb_status_id)
EXECUTE FUNCTION data_catalog.tg_status_object_restriction();

COMMENT ON TRIGGER tg_status_object_restriction 
ON data_catalog.tb_columns 
IS 'Trigger responsável por validar e restringir as transições de status dos objetos do catálogo lógico na tabela tb_columns, garantindo que colunas só avancem no workflow quando estiverem devidamente documentadas e aprovadas.';

-- Trigger responsável por tornar dinâmica a mudança de tb_status_id
-- para todos os schemas, tabelas e colunas que dependem de tb_databases.
-- Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6
CREATE TRIGGER tg_catalog_object_status_change_hierarchy BEFORE UPDATE ON data_catalog.tb_databases
FOR EACH ROW WHEN (NEW.tb_status_id IN (2,3,6))
EXECUTE PROCEDURE data_catalog.tg_catalog_object_status_change_hierarchy('databases');

COMMENT ON TRIGGER tg_catalog_object_status_change_hierarchy
ON data_catalog.tb_databases
IS 'Trigger responsável por tornar dinâmica a mudança de tb_status_id para todos os schemas, tabelas e colunas que dependem de tb_databases. Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6';

-- Trigger responsável por tornar dinâmica a mudança de tb_status_id
-- para todos os databases, tabelas e colunas que dependem de tb_schemas.
-- Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6
CREATE TRIGGER tg_catalog_object_status_change_hierarchy BEFORE UPDATE ON data_catalog.tb_schemas
FOR EACH ROW WHEN (NEW.tb_status_id IN (2,3,6))
EXECUTE PROCEDURE data_catalog.tg_catalog_object_status_change_hierarchy('schemas');

COMMENT ON TRIGGER tg_catalog_object_status_change_hierarchy
ON data_catalog.tb_schemas
IS 'Trigger responsável por tornar dinâmica a mudança de tb_status_id para todos os databases, tabelas e colunas que dependem de tb_schemas. Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6';

-- Trigger responsável por tornar dinâmica a mudança de tb_status_id
-- para todos os databases, schemas e colunas que dependem de tb_tables.
-- Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6
CREATE TRIGGER tg_catalog_object_status_change_hierarchy BEFORE UPDATE ON data_catalog.tb_tables
FOR EACH ROW WHEN (NEW.tb_status_id IN (2,3,6))
EXECUTE PROCEDURE data_catalog.tg_catalog_object_status_change_hierarchy('tables');

COMMENT ON TRIGGER tg_catalog_object_status_change_hierarchy
ON data_catalog.tb_tables
IS 'Trigger responsável por tornar dinâmica a mudança de tb_status_id para todos os databases, schemas e colunas que dependem de tb_tables. Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6';

-- Trigger responsável por tornar dinâmica a mudança de tb_status_id
-- para todos os databases, schemas e tabelas que dependem de tb_columns.
-- Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6
CREATE TRIGGER tg_catalog_object_status_change_hierarchy BEFORE UPDATE ON data_catalog.tb_columns
FOR EACH ROW WHEN (NEW.tb_status_id IN (2,3,6))
EXECUTE PROCEDURE data_catalog.tg_catalog_object_status_change_hierarchy('columns');

COMMENT ON TRIGGER tg_catalog_object_status_change_hierarchy
ON data_catalog.tb_columns
IS 'Trigger responsável por tornar dinâmica a mudança de tb_status_id para todos os databases, schemas e tabelas que dependem de tb_columns. Esta trigger só será executada caso o novo tb_status_id seja 2, 3 ou 6';

-- Trigger responsável por tornar dinâmica a mudança da coluna active
-- para todos os schemas, tabelas e colunas que dependem de tb_databases.
CREATE TRIGGER tg_inactivating_catalog_objects AFTER UPDATE ON data_catalog.tb_databases
FOR EACH ROW WHEN (NEW.active = FALSE)
EXECUTE PROCEDURE data_catalog.tg_inactivating_catalog_objects('databases');

COMMENT ON TRIGGER tg_inactivating_catalog_objects
ON data_catalog.tb_databases
IS 'Trigger responsável por tornar dinâmica a mudança da coluna active para todos os schemas, tabelas e colunas que dependem de tb_databases';

-- Trigger responsável por tornar dinâmica a mudança da coluna active
-- para todas as tabelas e colunas que dependem de tb_schemas.
CREATE TRIGGER tg_inactivating_catalog_objects AFTER UPDATE ON data_catalog.tb_schemas
FOR EACH ROW WHEN (NEW.active = FALSE)
EXECUTE PROCEDURE data_catalog.tg_inactivating_catalog_objects('schemas');

COMMENT ON TRIGGER tg_inactivating_catalog_objects
ON data_catalog.tb_schemas
IS 'Trigger responsável por tornar dinâmica a mudança da coluna active para todas as tabelas e colunas que dependem de tb_schemas';

-- Trigger responsável por tornar dinâmica a mudança da coluna active
-- para todas as colunas que dependem de tb_tables.
CREATE TRIGGER tg_inactivating_catalog_objects AFTER UPDATE ON data_catalog.tb_tables
FOR EACH ROW WHEN (NEW.active = FALSE)
EXECUTE PROCEDURE data_catalog.tg_inactivating_catalog_objects('tables');

COMMENT ON TRIGGER tg_inactivating_catalog_objects
ON data_catalog.tb_tables
IS 'Trigger responsável por tornar dinâmica a mudança da coluna active para todas as colunas que dependem de tb_tables';

-- Trigger responsável por tornar dinâmica a mudança da coluna active
-- para todas as colunas.
CREATE TRIGGER tg_inactivating_catalog_objects AFTER UPDATE ON data_catalog.tb_columns
FOR EACH ROW WHEN (NEW.active = FALSE)
EXECUTE PROCEDURE data_catalog.tg_inactivating_catalog_objects('columns');

COMMENT ON TRIGGER tg_inactivating_catalog_objects
ON data_catalog.tb_columns
IS 'Trigger responsável por tornar dinâmica a mudança da coluna active para todas as colunas';
