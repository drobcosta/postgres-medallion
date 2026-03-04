# Changelog

Todas as mudanças relevantes deste projeto serão documentadas aqui.

O formato segue o padrão [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)  
e este projeto adota **Semantic Versioning**.

## [1.0.0] - 2026-03-04
### Added
- Estrutura completa do catálogo lógico:
  - tb_status
  - tb_payload_period
  - tb_databases
  - tb_schemas
  - tb_tables
  - tb_data_types
  - tb_columns
- View consolidada `vw_catalog`
- Trigger `tg_status_object_restriction`
- Documentação completa do catálogo
- Automação da camada bronze (função `bronze_layer`)
- README inicial com arquitetura e workflows

### Changed
- Ajustes de padronização nos comentários SQL

### Fixed
- Correções de relacionamentos FK entre tabelas

## [0.1.0] - 2026-02-28
### Added
- Estrutura inicial do repositório
- Licença MIT
