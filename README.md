# Visão Geral do Projeto

O ecommerce-pipeline é uma pipeline de dados completa, que simula um e-commerce, desde a geração de dados sintéticos até a análise em dashboards.
Objetivos principais:

  - Simular dados realistas de usuários, produtos, pedidos e pagamentos.
  - Automatizar a ingestao diaria dos dados com Airflow.
  - Armazenamento inteligente em um data lake.
  - Processar dados distribuídos com PySpark.
  - Garantir qualidade de dados com Great Expectations.
  - Persistir dados confiáveis no Data Warehouse (PostgreSQL).
  - Monitorar e visualizar métricas de processos com Grafana e Prometheus.

---

## Stack Tecnológica

- **Orquestração:** Apache Airflow 3.0
- **Containers:** Docker Compose
- **Armazenamento de dados:** MinIO (buckets `raw` e `processed`)
- **Processamento:** PySpark (distribuído)
- **Formato de dados:** CSV e Parquet
- **Data Quality:** Great Expectations
- **Data Warehouse:** PostgreSQL
- **BI / Visualização:** Metabase
- **Monitoramento e métricas:** Grafana + Prometheus
- **Linguagem:** Python 3.12

---

## Estrutura do Projeto

```lua
├── docker
│   ├── airflow
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── prometheus
│   │   └── prometheus.yaml
│   └── spark
├── include
│   └── 2025-09-05
│       ├── orders.csv
│       ├── payments.csv
│       ├── products.csv
│       └── users.csv
├── mnt
│   ├── airflow
│   │   ├── config
│   │   ├── dags
│   │   │   ├── etl.py
│   │   │   └── generator_dag.py
│   │   ├── logs
│   │   └── plugins
│   ├── minio
│   ├── python
│   │   ├── data_generator.py
│   │   ├── __init__.py
│   │   └── minio_uploader.py
│   └── spark
│       ├── __init__.py
│       ├── orders.py
│       ├── payments.py
│       ├── products.py
│       ├── settings.py
│       └── users.py
├── README.txt
└── services
    ├── conf
    ├── datalake_dwh.yaml
    └── orchestration.yaml
```

---

## Descrição do Pipeline

1. **Ingestão (Raw)**  
   - Dados sintéticos gerados via `DataGenerator`:
     - `users.csv` → clientes
     - `products.csv` → produtos
     - `orders.csv` → pedidos
     - `payments.csv` → pagamentos
   - Todos os arquivos são salvos no bucket `raw/` do MinIO em **formato Parquet**.

2. **Transformação (Processed)**  
   - Cada dataset é processado individualmente com PySpark:
     - **Users:** deduplicação, padronização de e-mails e IDs
     - **Products:** normalização de preços, categorias e estoque
     - **Orders:** validação de datas, status e total_price
     - **Payments:** reconciliação com orders, validação de valores
   - Os dados transformados são salvos no bucket `processed/` do MinIO em **formato Parquet**.

3. **Validação de Qualidade (Great Expectations)**  
   - Cada dataset processado possui uma task GX separada para validação de regras de negócio:
     - Integridade de chaves
     - Valores nulos ou inválidos
     - Consistência de métricas

4. **Carga no Data Warehouse**  
   - Tabelas limpas e validadas são carregadas no PostgreSQL para análise e BI via Metabase.

5. **Observabilidade**  
   - Métricas do Airflow, Spark e containers coletadas pelo Prometheus e visualizadas no Grafana.

---

## Configuração de Credenciais
- As credenciais (MinIO, PostgreSQL, etc.) estão armazenadas em `.env` na pasta `conf/` .credentials.env`.

---

## Como Rodar

1. iniciar os containers com o makefile:
```bash
make up
```

2. Acesse o Airflow webserver
  - http://localhost:8080

3. Monitorar metricas:
- Grafana → http://localhost:3000
- Metabase → http://localhost:3001


## Em construção