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
│       └── Dockerfile
├── include
│   ├── 2025-09-05
│   │   ├── orders.csv
│   │   ├── payments.csv
│   │   ├── products.csv
│   │   └── users.csv
│   └── 2025-09-10
│       ├── orders.csv
│       ├── payments.csv
│       ├── products.csv
│       └── users.csv
├── Makefile
├── mnt
│   ├── airflow
│   │   ├── config
│   │   │   └── airflow.cfg
│   │   ├── dags
│   │   │   ├── ecommerce_etl.py
│   │   │   ├── etl.py
│   │   │   ├── generator_dag.py
│   │   │   └── spark_dag_test.py
│   │   ├── logs
│   │   └── plugins
│   ├── great_expectations
│   │   └── gx
│   │       ├── checkpoints
│   │       ├── expectations
│   │       ├── great_expectations.yml
│   │       ├── plugins
│   │       ├── uncommitted
│   │       └── validation_definitions
│   ├── minio
│   │   └── raw
│   │       ├── orders
│   │       ├── payments
│   │       ├── products
│   │       └── users
│   ├── python
│   │   ├── data_generator.py
│   │   ├── __init__.py
│   │   └── minio.py
│   └── spark
│       ├── __init__.py
│       ├── main.py
│       ├── postgres_ingestor.py
│       └── utils
│           ├── gx_validator.py
│           ├── __init__.py
│           ├── __pycache__
│           ├── spark_session.py
│           └── transformer.py
├── README.md
└── services
    ├── conf
    ├── datalake_dwh.yaml
    ├── observability.yaml
    ├── orchestration.yaml
    └── processing.yaml
```

---

## Descrição de como funciona
- Dag 1: 
  1. **Data source generator**
    - A dag generator_dag inicia o codigo para gerar os dados sinteticos via class `DataGenerator`
      - `users.csv`
      - `products.csv`
      - `orders.csv`
      - `payments.csv`
    - Todos os arquivos são salvos localmente em `include/`

- Dag 2: 
  2. **Ingestão (Raw)**  
    - Dados sintéticos .csv são carregados no bucket `raw/` do MinIO em **formato Parquet**.:
      - A ideia é focar em reduzir o custo de armazenamento, aplicando algumas boas praticas de otimização de arquivo:
        - Compressão com Snappy = menos espaço em disco, leitura rápida.
        - Codificação por dicionário = ótimo para colunas repetitivas, economia grande.
        - Redução de precisão de timestamps = menor armazenamento desnecessário.
        - Controle do tamanho de páginas = equilíbrio entre compressão e acesso seletivo eficiente.

  3. **Transformação (Processed)**  
      - Cada dataset é processado individualmente com PySpark, com inferência de schema seguida de cast explícito para garantir tipos corretos:
        - Users: deduplicação, padronização de e-mails, IDs e datas; remoção de registros sem user_id ou email; limpeza de nomes, cidade e estado.
        - Products: deduplicação, normalização de categorias e nomes, validação de price > 0 e stock >= 0.
        - Orders: deduplicação, validação de datas (order_date <= current_timestamp()), quantity > 0, total_price > 0; padronização de status; cast de IDs e valores.
        - Payments: deduplicação, validação de datas (paid_at <= current_timestamp()), amount > 0; padronização de métodos de pagamento; cast de IDs, valores e timestamps.
      - Os dados transformados são salvos no bucket processed/ do MinIO em formato Parquet com organização por ano, mês e dia.

  4. **Validação de Qualidade (Great Expectations)**  
    - Cada dataset processado possui uma task GX separada para validação de regras de negócio:
      - Integridade de chaves
      - Valores nulos ou inválidos
      - Consistência de métricas

  5. **Carga no Data Warehouse**  
    - Tabelas limpas e validadas são carregadas no PostgreSQL para análise e BI via Metabase.

6. **Observabilidade**  
   - Métricas do Airflow, Spark e docker coletadas pelo Prometheus e visualizadas no Grafana.

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