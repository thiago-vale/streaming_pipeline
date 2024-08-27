from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json
from delta import configure_spark_with_delta_pip
import logging

# configuração dos logs do pipeline
logging.basicConfig(
    filename='path/to/logs/pipeline-logs/pipeline.log',  # Nome do arquivo de log
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

extra = ['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2']

builder = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.eventLog.dir", "path/to/logs/spark-events") \
    .config("spark.eventLog.logBlockUpdates.enabled", "true") \
    .config("spark.eventLog.jsonFormat.enabled", "true")

logger.info('init Spark Session')
try:
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()

    logger.info('Read Stream')
    # Leitura dos dados do Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "host") \
        .option("subscribe", "topic") \
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
        .option("checkpointLocation", "path/to/checkpoint") \
        .start('path/to/delta/table')\
        .awaitTermination()
    
except Exception as e:
    logger.info(f'error: {e}')
