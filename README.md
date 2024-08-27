# Streaming Pipeline
Pipeline para ingestão de dados de Streaming

### Descrição

Pipeline de streaming  desenvolvido para processar dados em tempo real utilizando uma arquitetura escalavel baseada em tecnologias como Azure HDInsight, Databricks, Spark e Delta Lake

[Linda da Imagem](https://drive.google.com/file/d/1n3BJTERtmIW42N8HlJwptYRgKgs7W885/view?usp=sharing)

![Arquitetura](https://github.com/thiago-vale/streaming_pipeline/blob/master/Captura%20de%20tela%20de%202024-08-27%2020-01-40.png)

### Objetivo
Fazer a Ingestão e processamento de dados em tempo real de grandes volumes de dados a partir do sistema transacional

### Ferramentas

- **Azure HDInsight**: Serviço totalmente gerenciado para análise de big data que possibilita a criação de um cluster kafka gerenciado.
- **Kafka**: Plataforma de Streaming, utilizada para constuir pipelines em tempo real e aplicações de streaming.
- **Databricks**: Plataforma de dados que oferece um ambiente de colaboração para processamento e Analise de Dados, integrada com Spark.
- **Spark**: Motor de processamento de Big Data, usado aqui para processar os dados Atraves do Spark Streaming.
- **Azure Data Lake**: Sistema para armazenamento de dados em nuvem de forma escalavel e segura.
- **Delta Lake**: Camada de armazenamento que traz confiabilidade, permitindo transações ACID e outras funcionalidades.

### Estrutura do Pipeline

1. **Sistema Transacional**
    - Dados Gerados no sistema trasacional ( banco de dados SQL/NoSQL ) enviados em formado json para o kafka.

2. **Ingestão de Dados**
    - O Kafka, hospedado no Azure HDInsight, recebe e distribui as mensagens de dados em tempo real.

3. **Processamento**
    - O Spark Streaming, rodando em um cluster Databricks, consome as mensagens e processa os dados executando as tranformações.

4. **Armazenamento**
    - Dados processados e armazenados em três camada:
        - **Bronze**: Dados brutos iguais aos que foram ingeridos.
        - **Silver**: Dados limpos com algumas poucas tranformações prontos para analises.
        - **Gold**: Dados Transformados e enriquecidos prontos para consumo.
    - O armazenamento no Azure Data Lake, utilizando Delta Lake Para garantir a consistência.

5. **Serviços de Consumo**
    - Dados armanezados na Camada gold são catalogados e consumidos por Dashboards e Machine Learning permitindo insights em tempo real.

'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json
from delta import configure_spark_with_delta_pip
import logging

# configuração dos logs do pipeline
logging.basicConfig(
    filename='./logs/pipeline-logs/pipeline.log',  # Nome do arquivo de log
    level=logging.INFO,       # Nível de logging
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

extra = ['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2']

builder = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.eventLog.dir", "./logs/spark-events") \
    .config("spark.eventLog.logBlockUpdates.enabled", "true") \
    .config("spark.eventLog.jsonFormat.enabled", "true")

logger.info('init Spark Session')
try:
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()

    logger.info('Read Stream')
    # Leitura dos dados do Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "meu-topico") \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info('Structure Schema')

    # Adicionar o esquema esperado para os dados JSON
    schema = StructType([
        StructField('TransactionNo', StringType(), True),
        StructField('Date', StringType(), True),
        StructField('ProductNo', StringType(), True),
        StructField('ProductName', StringType(), True),
        StructField('Price', FloatType(), True),
        StructField('Quantity', IntegerType(), True),
        StructField('CustomerNo', FloatType(), True),
        StructField('Country', StringType(), True),
    ])

    # Aplicar o esquema e selecionar os dados
    df = df.select(from_json(df.value.cast("string"), schema).alias("data")).select("data.*")

    logger.info('Write Data')
    # Gravar os dados em Delta Lake
    query_delta = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint") \
        .start('./delta/table')\
        .awaitTermination()
    
except Exception as e:
    logger.info(f'error: {e}')
'''