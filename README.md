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
│   ├── 2025-09-10
│   │   ├── orders.csv
│   │   ├── payments.csv
│   │   ├── products.csv
│   │   └── users.csv
│   └── 2025-09-12
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
│       ├── load.py
│       ├── processing.py
│       └── utils
│           ├── gx_validator.py
│           ├── __init__.py
│           ├── load_postgres.py
│           ├── __pycache__
│           ├── spark_session.py
│           └── transformation.py
├── README.md
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
Pipeline ETL para ingestão, transformação, validação e carga em Postgres usando Airflow 3.0, MinIO (S3), PySpark e Great Expectations.  

  1. **Extract (Ingestão para raw/)**
       - **FileSensor Deferrable**
         - Aguarda arquivos no diretório `include/{execution_date}` sem ocupar slot do worker (`mode="reschedule"`).
       - **Params no DAG**
         - Permitem selecionar `execution_date` diretamente pela UI para reprocessamentos rápidos.  
      - **`list_csv_files(date)`**  
        - Identifica todos os arquivos CSV no diretório de staging.  
      - **`upload_file_to_minio`**  
        - Faz upload dos CSV convertendo para Parquet, utilizando **dynamic task mapping** (`.partial().expand(file_path=files)`) para paralelizar uploads.  
      - **Boas práticas de Parquet**  
        - (Snappy, dictionary encoding, page size ajustado) aplicadas no `MinioUtils.upload_df_as_parquet()`.  


  2. **Transform (Processamento com PySpark)**  
      - **`list_raw_files()`**  
        - Lista arquivos Parquet no bucket `raw/` via `MinioUtils`.  
      - **`build_spark_args()`**  
        - Constrói argumentos individuais para cada arquivo a ser processado.  
      - **Execução Spark Paralela**  
        - `SparkSubmitOperator.partial().expand(application_args=[[f] for f in files])` executa transformações PySpark de forma dinâmica sobre cada arquivo no MinIO.  
        - Configurações S3 (s3a), jars (hadoop-aws, aws-sdk) e credenciais são passadas via `conf` do Spark.  
      - **Isolamento Natural**  
        - Cada arquivo é processado individualmente; falhas não interrompem os demais arquivos.  


  3. **Validação de Qualidade (Great Expectations)**  
      - **TaskGroup `validation`**  
        - Agrupa todas as validações GX num bloco visual do UI.  
      - **`check_validation`**  
        - Expandida dinamicamente para cada tabela (`orders`, `payments`, `products`, `users`) usando `.partial().expand(table=table_list)`.  
      - **`MinioUtils.object_validation()`**  
        - Lista objetos no bucket usando `list_objects_v2`;  
        - Lê arquivos JSON de resultados das validações;  
        - Retorna dicionário `{success: bool, details: {...}}`.  
      - **Falhas Isoladas**  
        - Cada tabela é validada separadamente. Se uma falhar, apenas aquela task quebra (não derruba a pipeline inteira).  
        
  4. **Carga no Data Warehouse**
      - **`list_processed_bucket()`**  
        - Lista arquivos já processados no bucket `processed/`.  
      - **`build_spark_args()`**  
        - Prepara argumentos para cada arquivo processado.  
      - **Execução Spark Paralela**  
        - `SparkSubmitOperator.partial().expand(application_args=spark_args)` carrega dados de `processed/` para tabelas do Postgres usando PySpark.  
        - Conexão configurada com o driver PostgreSQL (`postgresql-42.7.5.jar`).  
      - **Pipeline Completa**  
        - Dados confiáveis pós-validação são carregados em tabelas prontas para consumo no Metabase ou outro BI.  

## Observabilidade e Operações
  - Métricas do Airflow, Spark e containers coletadas pelo Prometheus; dashboards configurados no Grafana.  
  - Logs integrados ao Airflow (`LoggingMixin`) para centralizar rastreabilidade.  
  - SLAs **podem ser aplicados** em tasks críticas (`sla=timedelta(...)`) para alertas automáticos.  
  - Sensores deferrables já implementados; uso de `short_circuit` **planejado** para reduzir ocupação de recursos e melhorar escalabilidade.

---
## Configuração do docker
- Para evitar erros como `SIGKILL` devido a falta de memória, configure os recursos do Docker da seguinte forma (especialmente em WSL2):
- Salvar em Usuarios/"nome_usuario"/.wslconfig
```ini
[wsl2]
memory=9GB       # Memória disponível para o Docker
processors=4     # Número de CPUs disponíveis para o Docker
swap=9GB         # Espaço de swap
```
## Configuração de Credenciais
- As credenciais (MinIO, PostgreSQL, etc.) estão armazenadas em `.env` na pasta `conf/` .credentials.env`.

---

## Como Rodar
1. Clone este repositório
```bash
git clone https://github.com/lobobranco96/ecommerce-pipeline.git
cd ecommerce-pipeline
```

2. Iniciar os containers:
  - Se tiver o makefile instalado
```bash
make up
```
  ou 
```bash
docker compose -f services/datalake_dwh.yaml up -d
docker compose -f services/orchestration.yaml up -d
docker compose -f services/processing.yaml up -d
docker compose -f services/observability.yaml up -d
```

3. Acesse o Airflow webserver
  - http://localhost:8080

4. Monitorar metricas:
- Grafana → http://localhost:3000


## Em construção
