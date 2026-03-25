![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue)
![Status: Active](https://img.shields.io/badge/Status-Active-brightgreen)
![Architecture: Medallion](https://img.shields.io/badge/Architecture-Medallion-orange)

# PostgreSQL Medallion
Automação e funções avançadas em PostgreSQL (plpgsql) para criação, gestão e governança de uma arquitetura medalhão (raw, bronze, silver, gold) em data warehouses. Inclui catálogo lógico, workflows de status e sincronização automática entre camadas.

# 🟦 PostgreSQL Medallion Architecture (RAW → BRONZE)
### *Automação completa da camada RAW e BRONZE usando apenas PostgreSQL*

---

## 📘 Visão Geral

Este repositório implementa uma arquitetura **Medallion** (RAW → BRONZE → SILVER → GOLD), com foco exclusivo nas camadas:

- **RAW**
- **BRONZE**

As camadas **SILVER** e **GOLD** **não fazem parte deste projeto**.

O objetivo é construir uma plataforma totalmente automatizada, capaz de:

- Detectar objetos na camada RAW  
- Catalogar automaticamente databases, schemas, tabelas e colunas  
- Criar estruturas BRONZE correspondentes  
- Executar *backfill* completo (full load inicial)  
- Executar CDC periódico (incremental)  
- Controlar governança via workflow  
- Garantir consistência, idempotência e auditabilidade  

Tudo isso usando **apenas PostgreSQL**, sem Spark, Airflow, Debezium, Kafka, DBT ou ferramentas externas.

---

## 🏛️ Arquitetura Geral

```
Debezium/Apache Nifi/Glue/Kinesis/AWS DMS/DataStream
 ↓
RAW
 ↓
AUDIT/HSTLOG (insert/update/delete)
 ↓
raw_into_catalog()
 ↓
Catálogo Lógico (metadados)
 ↓
Workflow + Governança (status + triggers)
 ↓
bronze_layer() (objetos criados em camada bronze)
 ↓
bronze_backfill_control
 ↓
bronze_backfill()
 ↓
BRONZE (estado final consistente)
 ↓
bronze_layer_control
 ↓
camada BRONZE em CDC com payloads periódicos
```

---

## 🗂️ Componentes do Sistema

### 1. Catálogo Lógico (Metadados)

Localizado no schema `data_catalog`, contém:

- `tb_status`
- `tb_payload_period`
- `tb_databases`
- `tb_schemas`
- `tb_tables`
- `tb_columns`
- `tb_data_types`
- `vw_catalog`

Esse catálogo é o **cérebro da plataforma**, responsável por governança, documentação e orquestração.

---

### 2. Ingestão Automática de Metadados RAW

A função: `data_catalog.raw_into_catalog()`

Detecta automaticamente:

- databases  
- schemas  
- tabelas  
- colunas  
- PKs  

A partir de schemas RAW com padrão: `[database]_[schema]_raw`

E popula o catálogo lógico de forma incremental e idempotente.

---

### 3. Governança e Workflow

Workflow completo de status:

1. NOVO OBJETO (ANALISAR)  
2. APROVADO DATA PLATFORM  
3. REPROVADO DATA PLATFORM  
4. EM CRIAÇÃO DATA PLATFORM  
5. DISPONÍVEL DATA PLATFORM  
6. APROVADO PARA REMOÇÃO DATA PLATFORM  
7. EM REMOÇÃO DATA PLATFORM  
8. REMOVIDO DATA PLATFORM  

As funções:

- `tg_status_object_restriction()`  
- `tg_catalog_object_status_change_hierarchy()`  

Garantem:

- transições válidas  
- propagação hierárquica  
- consistência entre database → schema → table → column  

Os Analistas de Dados realizam a gestão dos objetos que deverão ser criados na camada BRONZE.
Para que os objetos sejam aprovados para mudança de tb_status_id, algumas regras devem ser seguidas:

- tb_databases.description NÃO DEVE SER NULL
- tb_schemas.description NÃO DEVE SER NULL
- tb_tables.description NÃO DEVE SER NULL
- tb_tables.tb_payload_period_id NÃO DEVE SER NULL
- tb_columns.description NÃO DEVE SER NULL
- tb_columns.data_type NÃO DEVE SER NULL

Uma vez que os objetos estão aprovados e entram no Workflow, eles poderão entrar no motor de separação (apenas alteração para um tb_status_id intermediário) para criação ou remoção da camada bronze como também em operações de criação ou remoção do objeto na camada bronze.

Função responsável pela engenharia do Workflow: `data_catalog.bronze_layer`

---

### 4. Backfill (Full Load Inicial)

Controlado pela tabela: `data_catalog.bronze_backfill_control`


Executado pelas funções:

- `bronze_backfill_inserts()`  
- `bronze_backfill_updates()`  
- `bronze_backfill_deletes()`  
- `backfill_done()`  
- `bronze_backfill()`  

A view: `vw_bronze_backfill`


lista tabelas pendentes de backfill.

O backfill:

- lê tabelas audit/hstlog  
- aplica inserts, updates e deletes históricos  
- usa chunking automático  
- garante consistência mesmo com RAW mudando  

---

### 5. CDC Periódico (Incremental)

Após o backfill, o CDC periódico é controlado pela tabela: `data_catalog.bronze_layer_control`
E executado pela função: `SELECT data_catalog.bronze_payload();`


O CDC:

- respeita periodicidade configurada  
- processa apenas mudanças novas  
- mantém BRONZE sincronizado continuamente  

---

## 🔄 Fluxos Operacionais

### Fluxo RAW → Catálogo

```
RAW
↓
raw_into_catalog()
↓
Catálogo Lógico
```


### Fluxo Catálogo → BRONZE (Criação)

```
Status 2 → 4 → 5
↓
bronze_layer()
↓
Criação de schemas/tabelas/colunas BRONZE
```


### Fluxo Catálogo → BRONZE (Remoção)

```
Status 6 → 7 → 8
↓
bronze_layer()
↓
Remoção de objetos BRONZE
```


### Fluxo Backfill

```
hstlog_insert
hstlog_update
hstlog_delete
↓
bronze_backfill()
↓
bronze_backfill_control
↓
BRONZE (estado final consistente)
```


### Fluxo CDC Periódico

```
RAW (novas mudanças)
↓
bronze_layer_control
↓
bronze_payload_inserts()
bronze_payload_updates()
bronze_payload_deletes()
bronze_payload()
↓
BRONZE atualizado
```


---

## 🧱 Estrutura das Tabelas Audit/HSTLOG

Para cada tabela RAW:
```
schema_raw.table
schema_raw_hstlog.table_hstlog
```


Partições:

- `_insert`
- `_update`
- `_delete`
- `_truncate`
- `_default`

Cada registro contém:

- JSONB OLD  
- JSONB NEW  
- executed_at  

Isso permite reconstruir o estado final da tabela BRONZE com precisão absoluta.

---

## ⚙️ Instalação

### Pré-requisitos
- PostgreSQL 14+  
- Permissão para criar schemas, funções e triggers  
- Scheduler externo (pg_cron, Airflow, Rundeck, Buddy, etc.)  

### Execução dos scripts
Execute na ordem:

```
step_01_.sql
step_02_.sql
...
step_17_*.sql
```


---

## 🚀 Execução Automática

### Ingestão de metadados RAW

`SELECT data_catalog.raw_into_catalog();`

### Aprovação dos tb_status_id de objetos no catálogo

### Criação/remoção de objetos BRONZE

```
SELECT data_catalog.bronze_layer(2);
SELECT data_catalog.bronze_layer(4);
SELECT data_catalog.bronze_layer(6);
SELECT data_catalog.bronze_layer(7);
```

### Backfill

`SELECT data_catalog.bronze_backfill();`


---

## 🧠 Racional Técnico

### Por que usar tabelas audit/hstlog?
Porque a camada RAW continua mudando durante o backfill.

### Por que chunking?
Para suportar tabelas gigantes.

### Por que workflow?
Para garantir governança e consistência.

### Por que IDs MD5?
Para garantir unicidade determinística.

### Por que tudo em PostgreSQL?
Para ambientes restritos e baixo custo.

---

## 🛠️ Guia de Contribuição

- Branches:
```
feature/*
fix/*
refactor/*
```

- Commits claros  
- Scripts versionados com prefixo `step_XX_`  
- Funções documentadas  

---

## 🎓 Onboarding para Novos Devs

1. Entender a arquitetura (este README)  
2. Explorar o catálogo lógico (`vw_catalog`)  
3. Entender o backfill (`bronze_backfill_control`)  
4. Entender o CDC (`bronze_layer_control`)  
5. Rodar o pipeline com as funções principais  

---

## 🧭 Roadmap

- [ ] Documentação SILVER  
- [ ] Documentação GOLD  
- [ ] CLI para gerenciar catálogo  
- [ ] Testes automatizados (pgTAP)  
- [ ] Monitoramento via Prometheus/Grafana  
- [ ] Geração automática de documentação  

---
