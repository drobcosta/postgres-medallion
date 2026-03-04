# Contribuindo para este projeto

Obrigado por considerar contribuir! Este repositório contém funções, automações e estruturas de governança em PostgreSQL para suportar arquiteturas medalhão (raw → bronze → silver → gold).  
Para manter a qualidade e consistência do projeto, siga as diretrizes abaixo.

## Como contribuir

### 1. Abra uma Issue
Antes de enviar qualquer alteração, crie uma **issue** descrevendo:
- O problema identificado  
- A proposta de melhoria  
- Scripts ou trechos relevantes  

Isso permite discussão e alinhamento antes da implementação.

## 2. Crie um Fork do repositório
Faça um fork e trabalhe em um branch separado:

```
git checkout -b feature/nome-da-feature
```

## 3. Padrões de código SQL

- Utilize **snake_case** para nomes de funções, tabelas e colunas.  
- Sempre inclua **COMMENT ON** para tabelas, colunas e views.  
- Evite funções sem schema (sempre usar `data_catalog.`).  
- Toda tabela deve possuir:
  - PK clara  
  - Campos `created_at` e `updated_at` quando aplicável  
  - FKs explícitas  
- Toda alteração no workflow deve ser refletida na trigger  
  `data_catalog.tg_status_object_restriction`.

## 4. Commits

Use mensagens claras e objetivas:
* Excemplo: `feat: adiciona validação de status para tb_columns`
* Excemplo: `fix: corrige FK de tb_tables`
* Excemplo: `docs: atualiza README com nova arquitetura`

## 5. Pull Requests

Ao abrir um PR:

* Relacione a issue correspondente  
* Explique o que foi alterado  
* Inclua exemplos de uso, quando aplicável  
* Garanta que o SQL está formatado e comentado  

PRs só serão aceitos se seguirem o workflow de governança.

## 6. Segurança e Boas Práticas

* Nunca incluir dados sensíveis ou reais.  
* Não subir dumps completos de bancos.  
* Não incluir credenciais, strings de conexão ou secrets.  
* Scripts devem ser idempotentes (podem rodar mais de uma vez sem quebrar).

## 7. Licença
Ao contribuir, você concorda que suas contribuições serão licenciadas sob a **MIT License**.
