# Automação de Validação Massiva de Acessos 🚀

Este projeto consiste em um pipeline de Engenharia de Dados e Automação desenvolvido em Python para validar a disponibilidade de ofertas em um portal web para uma base massiva de clientes (**~100.000 registros diários**).

O objetivo principal foi substituir verificações sequenciais ou manuais por uma solução de alta performance capaz de processar milhares de requisições por minuto, integrando os dados diretamente ao Data Warehouse corporativo.

## 🛠 Tecnologias Utilizadas

* **Python 3.10+**
* **Requests:** Para requisições HTTP leves e verificação de status code.
* **Concurrent Futures (ThreadPoolExecutor):** Implementação de paralelismo (Multithreading) para escalabilidade.
* **Google BigQuery (pandas-gbq):** Leitura (Input) e Escrita (Output) de dados na nuvem.
* **Pandas:** Manipulação, limpeza e transformação de dados (ETL).

## ⚙️ Arquitetura da Solução

1.  **Extração Dinâmica:** O script conecta ao BigQuery, identifica a tabela correta baseada na data de referência (lógica dinâmica de sufixos) e consolida os dados com o histórico de clientes (SCD Type 2).
2.  **Processamento Paralelo:**
    * Utiliza `ThreadPoolExecutor` para gerenciar 50 threads simultâneas.
    * Simula a validação de acesso no endpoint alvo.
3.  **Carga (Load):**
    * Gera um backup local em CSV para auditoria.
    * Envia os resultados enriquecidos de volta para uma tabela consolidada no BigQuery para consumo de dashboards.

## 📈 Resultados

* **Escalabilidade:** Capacidade comprovada de processar +100.000 requisições em tempo reduzido.
* **Eficiência:** Redução drástica no tempo de validação (SLA de horas para minutos).
* **Confiabilidade:** Enriquecimento da base de dados com feedback em tempo real, eliminando falhas humanas.

---

### ⚠️ Disclaimer / Observação Importante

**Nota sobre Confidencialidade e Dados:**

Este repositório contém uma versão sanitizada do código original utilizado em produção. Para garantir a segurança da informação e conformidade com as políticas corporativas:

* **Credenciais de Projeto (GCP):** Foram substituídas por placeholders genéricos (`your-gcp-project-id`).
* **Estrutura de Dados:** Nomes de tabelas e colunas sensíveis foram alterados para termos genéricos (`customer_id`, `phone_number`, `CAMPAIGN_BASE`).
* **Endpoints:** As URLs de API foram substituídas por endereços de exemplo.

A lógica de automação, arquitetura de paralelismo e manipulação de dados permanecem fiéis ao projeto original.
