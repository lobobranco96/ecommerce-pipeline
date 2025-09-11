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
│   │   │   ├── dev_dag
│   │   │   └── production
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
├── README.txt
└── services
    ├── conf
    ├── datalake_dwh.yaml
    ├── observability.yaml
    ├── orchestration.yaml
    └── processing.yaml
```

---

# E como o projeto funciona?
## Dag 1 - data_generator: 
  1. A dag data_generator, inicia o codigo para gerar os dados sinteticos via class `DataGenerator`
      - `users.csv`
      - `products.csv`
      - `orders.csv`
      - `payments.csv`
    - Todos os arquivos são salvos localmente em `include/`

## Dag 2 - ecommerce_etl: 
  1. **Extract (Ingestão para raw/)**
      - Sensor deferrable (FileSensor com mode="reschedule") que aguarda os arquivos em include/{execution_date} sem ocupar slot do worker.
      - Params do DAG permitem selecionar execution_date diretamente pela UI para reprocessamentos rápidos.
      - list_csv_files(date) identifica CSVs no diretório de staging.
      - Upload_file_to_minio faz o upload convertendo para Parquet. Implementado com dynamic mapping (.partial().expand(file_path=files)) para paralelizar uploads sem duplicar código.
      - Boas práticas de Parquet (Snappy, dictionary encoding, page size ajustado) são aplicadas ao salvar.

  3. **Transform (Processamento com PySpark)**  
      - list_raw_files() lista arquivos Parquet em raw/ via MinioUtils.
      - files_exist usa @task.short_circuit para interromper o fluxo cedo quando não há dados, evitando steps desnecessários.
      - SparkSubmitOperator.partial().expand(application_args=[[f] for f in files]) — Spark roda em paralelo sobre cada conjunto de arquivos.
      - Configurações S3 (s3a), jars (hadoop-aws, aws-sdk) e credenciais são passadas via conf do Spark ou via Connections (recomendado).

  4. **Validação de Qualidade (Great Expectations)**  
      - TaskGroup validation agrupa todas as validações GX, oferecendo um bloco visual no UI.
      - check_validation é expandida dinamicamente para cada tabela (orders, payments, products, users) usando .partial().expand(table=table_list).
      - MinioUtils.object_validation foi ajustado para:
      - listar objetos com list_objects_v2 (não usar curingas em get_object), ler o(s) arquivo(s) JSON de resultado e retornar um dicionário com success e details.
      - Falhas de validação resultam em ValueError na task correspondente — falha isolada (não derruba toda a pipeline).

  5. **Carga no Data Warehouse**
      - Após validações bem-sucedidas, dados confiáveis são carregados para o PostgreSQL (via Spark ou um ingestor dedicado).
      - Tabelas no DWH ficam prontas para consumo no Metabase.

**Observabilidade e Operações**  
  - Métricas do Airflow, Spark e containers coletadas pelo Prometheus; dashboards configurados no Grafana.
  - Logs integrados ao Airflow (LoggingMixin) para centralizar rastreabilidade.
  - SLAs podem ser aplicados em tasks críticas (sla=timedelta(...)) para alertas automáticos.
  - Sensores deferrables e short_circuit reduzem ocupação de recursos e melhoram escalabilidade.

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